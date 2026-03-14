# pkg/config — Target State

## Changes from Current

- [SAFE] Unexport `DefaultMembershipTTL`, `DefaultDelegationTTL`, `DefaultTLSIdentityTTL`, `DefaultReconnectWindow` — never referenced outside the package
- [SAFE] Replace `ed25519PublicKeyBytes` constant with `crypto/ed25519.PublicKeySize`
- [SAFE] Delete `BootstrapProtoPeers` nil-receiver guard
- [SAFE] Delete `RememberBootstrapPeer` nil-receiver guard

## Target Exported API

### Unexported constants

- `DefaultMembershipTTL` -> `defaultMembershipTTL`
- `DefaultDelegationTTL` -> `defaultDelegationTTL`
- `DefaultTLSIdentityTTL` -> `defaultTLSIdentityTTL`
- `DefaultReconnectWindow` -> `defaultReconnectWindow`

### Deleted internal constants

- `ed25519PublicKeyBytes` — replaced with `crypto/ed25519.PublicKeySize`

## Deleted Items

| Item | Reason |
|------|--------|
| `ed25519PublicKeyBytes` constant | Duplicates `crypto/ed25519.PublicKeySize` |
| Nil-receiver guards on `BootstrapProtoPeers` and `RememberBootstrapPeer` | `Config` is never nil from `Load` |
