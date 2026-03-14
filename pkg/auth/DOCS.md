# pkg/auth — Target State

## Changes from Current

- [SAFE] Absorb identity key I/O from `pkg/node`: `GenIdentityKey`, `ReadIdentityPub`, `loadIdentityKey`, `decodePubKeyPEM` (~100 LOC incoming)
- [SAFE] Unexport `EnsureNodeCredentialsFromToken` — only called by `LoadOrEnrollNodeCredentials` in same file
- [SAFE] Unexport `IsCapSubset` — only used internally and in tests
- [SAFE] Delete `VerifiedInviteToken` struct — return value discarded by all callers
- [SAFE] Simplify `VerifyDelegationCertChain` — export the inner function directly instead of wrapping

## Target Exported API

### New exports (moved from `pkg/node`)

- `GenIdentityKey(pollenDir string) (ed25519.PrivateKey, ed25519.PublicKey, error)`
- `ReadIdentityPub(pollenDir string) (ed25519.PublicKey, error)`

### Unexported (were exported)

- `EnsureNodeCredentialsFromToken` -> `ensureNodeCredentialsFromToken`
- `IsCapSubset` -> `isCapSubset`

### Changed exports

- `VerifyInviteToken(token, expectedSubject, now) error` — no longer returns `*VerifiedInviteToken` (callers discarded it)

### Deleted exports

- `VerifiedInviteToken` struct — return type no longer needed
- `VerifyDelegationCertChain` wrapper — inner function exported directly as `VerifyDelegationCertChain`

## Target File Layout

| File | Content | Est. LOC |
|------|---------|----------|
| `auth.go` | Core delegation cert issuance/verification, join tokens, admin keys, trust bundle, credentials — unchanged | ~816 |
| `invite.go` | Invite token issuance/verification, delegation signer — minus `VerifiedInviteToken` | ~285 |
| `identity.go` | **New file.** `GenIdentityKey`, `ReadIdentityPub`, `loadIdentityKey`, `decodePubKeyPEM`, PEM constants — moved from `pkg/node` | ~100 |

## Deleted Items

| Item | Reason |
|------|--------|
| `VerifiedInviteToken` struct | No production consumer reads its fields |
