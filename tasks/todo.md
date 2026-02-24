# Issues #13 + #14: Certificate TTLs & Revocation

## Problem

Certificates use hardcoded TTLs with no renewal and no revocation mechanism.
A compromised node retains valid credentials until cert expiry (up to 1 year)
with no way to evict it from the mesh.

## Acceptance Criteria

- [ ] Cert TTLs configurable via `config.yaml` (membership, admin, TLS identity)
- [ ] `pollen status` shows cert expiry info with pre-expiry warning
- [ ] Admin-signed revocation events gossip through the mesh
- [ ] Revoked peers rejected at TLS handshake
- [ ] `pollen revoke <peer>` CLI command
- [ ] All existing tests pass, new tests cover revocation

---

## Step 1: Proto definitions

### 1a. `api/public/pollen/admission/v1/admission.proto`

Add a `RevocationClaims` and `SignedRevocation` message pair:

```protobuf
message RevocationClaims {
  bytes cluster_id = 1;
  bytes subject_pub = 2;        // the revoked peer's public key
  bytes admin_pub = 3;          // admin key that signed this revocation
  int64 revoked_at_unix = 4;
}

message SignedRevocation {
  RevocationClaims claims = 1;
  bytes signature = 2;          // ed25519 context-sig over claims
}
```

### 1b. `api/public/pollen/state/v1/state.proto`

Add `RevocationChange` message and add it to the `GossipEvent.change` oneof:

```protobuf
message RevocationChange {
  bytes subject_pub = 1;
  pollen.admission.v1.SignedRevocation revocation = 2;
}
```

In `GossipEvent`:
```protobuf
oneof change {
  ...
  ReachabilityChange reachability = 7;
  RevocationChange revocation = 9;   // field 9; field 8 is `deleted`
}
```

### 1c. `api/public/pollen/control/v1/control.proto`

Add `RevokePeer` RPC and cert info to `GetStatusResponse`:

```protobuf
service ControlService {
  ...
  rpc RevokePeer(RevokePeerRequest) returns (RevokePeerResponse);
}

message RevokePeerRequest {
  bytes peer_id = 1;
}

message RevokePeerResponse {}

message CertificateInfo {
  int64 not_before_unix = 1;
  int64 not_after_unix = 2;
  string cert_type = 3;         // "membership", "admin"
}

// Add to GetStatusResponse:
// repeated CertificateInfo certificates = 5;
```

### 1d. Run `just generate`

Regenerate all proto code and fix any `tools.go` imports if needed.

---

## Step 2: Auth layer — revocation signing & verification

### 2a. `pkg/auth/auth.go`

Add signing context constant:
```go
sigContextRevocation = "pollen.revocation.v1"
```

Add two functions:

```go
func SignRevocation(adminPriv ed25519.PrivateKey, clusterID, subjectPub []byte) (*admissionv1.SignedRevocation, error)
```
- Build `RevocationClaims{ClusterId, SubjectPub, AdminPub, RevokedAtUnix: now}`
- Sign with `sigContextRevocation`

```go
func VerifyRevocation(rev *admissionv1.SignedRevocation, trust *admissionv1.TrustBundle) error
```
- Verify claims.AdminPub matches trust.RootPub (or is a delegated admin — for v1, just root)
- Verify signature with `sigContextRevocation`
- Verify cluster_id matches trust

### 2b. `pkg/auth/auth_test.go`

Add tests:
- `TestSignAndVerifyRevocation` — round-trip sign/verify
- `TestVerifyRevocationWrongKey` — forged signature
- `TestVerifyRevocationWrongCluster` — cluster mismatch

---

## Step 3: Configurable TTLs

### 3a. `pkg/config/config.go`

Add TTL fields to the `Config` struct:

```go
type CertTTLs struct {
    MembershipDays int `yaml:"membershipDays,omitempty"`
    AdminDays      int `yaml:"adminDays,omitempty"`
    TLSIdentityDays int `yaml:"tlsIdentityDays,omitempty"` //nolint:tagliatelle
}
```

Add to `Config`:
```go
type Config struct {
    BootstrapPeers []BootstrapPeer `yaml:"bootstrapPeers,omitempty"`
    CertTTLs       *CertTTLs       `yaml:"certTTLs,omitempty"`  //nolint:tagliatelle
}
```

Add defaults:
```go
const (
    DefaultMembershipDays  = 365
    DefaultAdminDays       = 1825  // 5 * 365
    DefaultTLSIdentityDays = 3650  // 10 * 365
)

func (c *Config) MembershipTTL() time.Duration { ... }
func (c *Config) AdminCertTTL() time.Duration { ... }
func (c *Config) TLSIdentityTTL() time.Duration { ... }
```

### 3b. Thread TTLs through to callers

- `cmd/pollen/main.go:runNode` — load config, pass TTLs to auth calls
- `pkg/auth/auth.go:EnsureLocalRootCredentials` — accept membership TTL param
- `pkg/auth/invite.go:resolveAdminIssuer` — accept admin TTL param
- `pkg/mesh/identity.go:generateIdentityCert` — accept TTL duration param instead of using const
- `pkg/mesh/mesh.go:NewMesh` — pass TLS identity TTL from config

The existing constants become fallback defaults; the config values override.

---

## Step 4: Store — revocation gossip event handling

### 4a. `pkg/store/gossip.go`

Add `attrRevocation` to `attrKind` enum:
```go
const (
    attrNetwork attrKind = iota + 1
    attrExternalPort
    attrIdentity
    attrService
    attrReachability
    attrRevocation
)
```

Add `revocationAttrKey`:
```go
func revocationAttrKey(subject types.PeerKey) attrKey {
    return attrKey{kind: attrRevocation, peer: subject}
}
```

Add revocation state to `Store`:
```go
type Store struct {
    ...
    revocations map[types.PeerKey]*admissionv1.SignedRevocation
    trustBundle *admissionv1.TrustBundle
}
```

Key design decisions:
- Revocations are keyed by **subject PeerKey**, not serial number
- Gossip revocations are **verified on receive** using `auth.VerifyRevocation`
- Revocations are **cluster-wide** but stored under the issuing node's gossip log

Update these functions (add `case attrRevocation:` / `*statev1.GossipEvent_Revocation`):
- `eventAttrKey` — extract subject PeerKey from `RevocationChange.SubjectPub`
- `applyDeleteLocked` — delete from `revocations` map
- `applyValueLocked` — call `auth.VerifyRevocation`, if valid add to `revocations` map
- `buildEventFromLog` — reconstruct `RevocationChange` from stored revocation

Add query method:
```go
func (s *Store) IsSubjectRevoked(subjectPub []byte) bool
```

Update `Load()` signature to accept trust bundle:
```go
func Load(pollenDir string, identityPub []byte, trustBundle *admissionv1.TrustBundle) (*Store, error)
```

Add local mutation method:
```go
func (s *Store) AddRevocation(rev *admissionv1.SignedRevocation) []*statev1.GossipEvent
```

### 4b. Update `Load()` callers (3 sites)

- `cmd/pollen/main.go:437` — pass `creds.Trust`
- `pkg/node/node_test.go:87` — pass `tn.creds.Trust`
- `pkg/store/gossip_test.go:newTestStore` — pass `nil` (tests that don't exercise revocation)

---

## Step 5: TLS handshake revocation check

### 5a. `pkg/mesh/identity.go`

Add an `isSubjectRevoked func([]byte) bool` field to `verifyMeshPeerOpts`:

```go
type verifyMeshPeerOpts struct {
    trustBundle      *admissionv1.TrustBundle
    expectedPeer     *types.PeerKey
    isSubjectRevoked func([]byte) bool
}
```

In `verifyMeshPeerCert`, after `auth.VerifyMembershipCert` succeeds:
```go
if opts.isSubjectRevoked != nil && opts.isSubjectRevoked(peerKey.Bytes()) {
    return errors.New("peer certificate revoked")
}
```

### 5b. `pkg/mesh/mesh.go`

Update `NewMesh` to accept the revocation checker and thread it through:
```go
func NewMesh(defaultPort int, signPriv ed25519.PrivateKey, creds *auth.NodeCredentials, isSubjectRevoked func([]byte) bool) (Mesh, error)
```

Pass `isSubjectRevoked` into every `verifyMeshPeerOpts` construction:
- `newServerTLSConfig` call in `Start`
- `newExpectedPeerTLSConfig` calls in `dialDirect` and `dialPunch`

### 5c. Update `NewMesh` callers

- `pkg/node/node.go:110` — pass `stateStore.IsSubjectRevoked`
- `pkg/mesh/mesh_test.go` — pass `nil` or a test stub

---

## Step 6: Node service — RevokePeer RPC + cert status

### 6a. `pkg/node/svc.go`

Add `RevokePeer` method to `NodeService`. Needs access to admin signer.

Update `NodeService` struct:
```go
type NodeService struct {
    controlv1.UnimplementedControlServiceServer
    node        *Node
    shutdown    func()
    adminSigner *auth.AdminSigner
    creds       *auth.NodeCredentials
}
```

Update `NewNodeService`:
```go
func NewNodeService(n *Node, shutdown func(), adminSigner *auth.AdminSigner, creds *auth.NodeCredentials) *NodeService
```

`RevokePeer` implementation:
1. Validate adminSigner is available
2. Call `auth.SignRevocation(signer.Priv, signer.Trust.ClusterId, req.PeerId)`
3. Call `n.store.AddRevocation(rev)` → get gossip events
4. Queue gossip events via `n.queueGossipEvents`

`GetStatus` addition:
- Read creds to build `CertificateInfo` for local membership cert
- Append to `GetStatusResponse.Certificates`

### 6b. Update `NewNodeService` callers (3 sites)

- `cmd/pollen/main.go:467` — pass `creds.InviteSigner, creds`
   (note: adminSigner is the same as `creds.InviteSigner` when present, nil otherwise)
- `pkg/node/node_test.go:121` — pass `nil, nil`
- `pkg/node/svc_test.go:15` — pass `nil, nil`

---

## Step 7: CLI — `pollen revoke` command

### 7a. `cmd/pollen/main.go`

Add `newRevokeCmd()`:
```go
func newRevokeCmd() *cobra.Command {
    return &cobra.Command{
        Use:   "revoke <peer-prefix>",
        Short: "Revoke a peer's membership",
        Args:  cobra.ExactArgs(1),
        Run:   runRevoke,
    }
}
```

`runRevoke`:
1. Call `GetStatus` to list all peers
2. Resolve peer by hex prefix (unique prefix match)
3. Ask for confirmation
4. Call `RevokePeer` RPC

Register in root command.

### 7b. `cmd/pollen/status.go`

Add `collectCertificatesSection` to show cert expiry with warning when <30 days.

---

## Step 8: Tests

### 8a. `pkg/store/gossip_test.go`

- Test `AddRevocation` produces correct gossip events
- Test applying revocation events from gossip updates `IsSubjectRevoked`
- Test that forged (wrong-signature) revocation events are rejected
- Test that revocations persist through `buildEventsAbove` round-trip

### 8b. `pkg/auth/auth_test.go`

- Test `SignRevocation` / `VerifyRevocation` happy path
- Test signature verification failure
- Test cluster ID mismatch

---

## Step 9: Build & test

```
just build
just test
```

Fix any lint issues (exhaustive switches, tagliatelle, mnd, nestif).

---

## Step 10: Commit & PR

Use `/commit` skill, push, create PR closing #13 and #14.

---

## Lessons from First Pass (reference)

These lessons are baked into the plan above:

1. **Key revocations by PeerKey, not serial** — serial isn't available via gossip
2. **Verify revocation signatures on receive** — Store needs TrustBundle
3. **Only `IsSubjectRevoked([]byte) bool`** — no `IsRevoked(serial)` needed
4. **`revocationAttrKey` uses `attrKey{kind: attrRevocation, peer: pk}`** — no string round-trip
5. **Add `attrRevocation` case to ALL exhaustive switches** — `applyDeleteLocked`, `buildEventFromLog`
6. **No dead code** — no `Revocations()` method, no `RenewalThresholdPercent`
7. **Use `nolint:tagliatelle` for mixed-case yaml tags**
8. **`store.Load` takes trustBundle** — update all 3 callers
9. **`NewNodeService` takes adminSigner + creds** — update all 3 callers
10. **`NewMesh` takes `isSubjectRevoked` callback** — update all callers

---

## Defluff Review Fixes

- [x] **1a** Assert inner/outer subject_pub match in `applyRevocationIfValid`
- [x] **1b** Remove redundant `adminSigner` field from `NodeService`
- [x] **2a** Add `now time.Time` param to `SignRevocation`
- [x] **2b** Deduplicate TTL defaults — single source of truth in `config`
- [x] **2c** Extract `certTTL` helper for copy-paste TTL methods
- [x] **3a** Replace stringly-typed `cert_type` with `CertType` proto enum
- [x] **3b** Add comment for field number gap in `GossipEvent`
- [x] **4a** Persist revocations to disk via `diskState.Revocations`
- [x] **5a** Revert cosmetic field reorders in `diskPeer`, `nodeRecord`, `KnownPeer`
- [x] **5b** Remove duplicate `nolint:tagliatelle`
- All tests pass, build clean
