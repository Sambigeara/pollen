# pkg/auth

## Responsibilities
- Manages node identity keys (ed25519 key pair generation/loading)
- Issues and verifies delegation certificates for role-based access control
- Creates and validates join tokens and invite tokens for node enrollment
- Manages trust bundles (cluster root public key and cluster ID)

## File Layout

| File | Content | Est. LOC |
|------|---------|----------|
| `auth.go` | Core delegation cert issuance/verification, join tokens, admin keys, trust bundle, credentials | ~816 |
| `invite.go` | Invite token issuance/verification, delegation signer | ~285 |
| `identity.go` | `GenIdentityKey`, `ReadIdentityPub`, `loadIdentityKey`, `decodePubKeyPEM`, PEM constants | ~100 |

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `NodeCredentials` | type | Trust bundle + delegation cert + signer key |
| `DelegationSigner` | type | Wraps signer key, issuer cert, trust, and invite consumer |
| `VerifiedToken` | type | Result of VerifyJoinToken with verified claims |
| `InviteConsumer` | interface | `TryConsume` method for tracking redeemed invite tokens |
| `FullCapabilities` | func | Returns admin capabilities (CanDelegate, CanAdmit, MaxDepth=255) |
| `LeafCapabilities` | func | Returns member capabilities (no delegation or admission) |
| `CertExpiresAt` | func | Extract expiration time from delegation cert |
| `IsCertExpiredAt` | func | Check if cert expired at time (with skew allowance) |
| `IsCertExpired` | func | Check if cert is expired at given time |
| `CertTTL` | func | Original TTL of cert (notAfter - notBefore) |
| `CertAccessDeadline` | func | Hard access deadline from cert claims |
| `IsCertWithinReconnectWindow` | func | Check if expired cert is within grace period |
| `LoadOrEnrollNodeCredentials` | func | Load existing credentials or enroll from join token |
| `EnsureLocalRootCredentials` | func | Ensure root credentials exist |
| `LoadExistingNodeCredentials` | func | Load and verify credentials |
| `SaveNodeCredentials` | func | Persist node credentials to disk |
| `LoadAdminKey` | func | Load root admin key from disk |
| `LoadOrCreateAdminKey` | func | Load or generate root admin key |
| `NewTrustBundle` | func | Create trust bundle from root public key |
| `IssueDelegationCert` | func | Issue signed delegation cert |
| `VerifyDelegationCertChain` | func | Verify proto validity, cluster scope, signature chain |
| `VerifyDelegationCert` | func | Verify delegation cert including time validity |
| `IssueJoinToken` | func | Admin issues join token for new node |
| `IssueJoinTokenWithIssuer` | func | Issue join token using delegated issuer cert |
| `VerifyJoinToken` | func | Verify join token signature and membership cert |
| `EncodeJoinToken` | func | Base64 encode join token |
| `DecodeJoinToken` | func | Base64 decode join token |
| `SaveDelegationCert` | func | Persist delegation cert to disk |
| `LoadDelegationSigner` | func | Load DelegationSigner from admin key or delegated cert |
| `IssueInviteTokenWithSigner` | func | Issue invite token using DelegationSigner |
| `VerifyInviteToken` | func | Verify invite token signature and claims |
| `EncodeInviteToken` | func | Base64 encode invite token |
| `DecodeInviteToken` | func | Base64 decode invite token |
| `MarshalDelegationCertBase64` | func | Encode delegation cert to base64 |
| `UnmarshalDelegationCertBase64` | func | Decode delegation cert from base64 |
| `GenIdentityKey` | func | Load or generate node ed25519 identity key pair |
| `ReadIdentityPub` | func | Read public key from disk |

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/perm | `EnsureDir`, `SetGroupReadable`, `WriteGroupReadable` |

## Consumed by
- pkg/mesh (uses: `DelegationSigner`, `NodeCredentials`, `CertExpiresAt`, `VerifyInviteToken`, `IssueJoinTokenWithIssuer`, `CertAccessDeadline`)
- pkg/node (uses: `NodeCredentials`, `IsCertExpired`, `IsCertWithinReconnectWindow`, `VerifyDelegationCert`, `SaveNodeCredentials`, `LoadOrEnrollNodeCredentials`, `EnsureLocalRootCredentials`, `LoadDelegationSigner`, `GenIdentityKey`, `ReadIdentityPub`)
- pkg/store (uses: `InviteConsumer`, `IsCertExpiredAt`)
- cmd/pln (uses: `VerifyJoinToken`, `NodeCredentials`, `LoadNodeCredentials`, `GenIdentityKey`, `IssueInviteToken`)
