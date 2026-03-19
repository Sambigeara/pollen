# Phase 1 Completion Checklist

Source of truth: `TARGET_ARCHITECTURE_SPEC.md`
Gap analysis: `GAP_ANALYSIS.md`

Every item must be verified against the current code. Mark DONE, PARTIAL, or OPEN.

---

## Supervisor

### Constructor
- [ ] Signature matches spec: `New(cfg config.Config, creds auth.NodeCredentials) (*Supervisor, error)` — or justified deviation documented
- [ ] No supervisor-local `Config` struct — uses `config.Config` (or deviation justified)
- [ ] `BootstrapPeer` and `ServiceEntry` types live in config package, not supervisor

### Exported methods on *Supervisor
- [ ] `Run(ctx context.Context) error` — present
- [ ] NO other exported methods beyond `Run` and `New` (or each extra is justified below)

#### Methods that MUST be removed or internalized:
- [ ] `Membership()` — removed (control wired internally)
- [ ] `Placement()` — removed (control wired internally)
- [ ] `Tunneling()` — removed (control wired internally)
- [ ] `TransportInfo()` — removed (control wired internally)
- [ ] `MetricsSource()` — removed (control wired internally)
- [ ] `ConnectPeer()` — removed (dead alias for Connect)
- [ ] `SeedWorkload()` — removed or moved into placement
- [ ] `Snapshot()` — removed (callers use StateStore or control layer)
- [ ] `AddDesiredConnection()` — removed (callers go through control layer)
- [ ] `RemoveDesiredConnection()` — removed (callers go through control layer)
- [ ] `DesiredConnections()` — removed (callers go through control layer)
- [ ] `JoinWithInvite()` — removed (callers go through control layer or transport directly)
- [ ] `Connect()` — removed (callers go through control.MeshConnector)

#### Methods with justified exceptions (test helpers — document why):
- [ ] `StateStore()` — if kept, document: integration test access only
- [ ] `Ready()` — if kept, document: integration test lifecycle sync
- [ ] `ListenPort()` — if kept, document: integration test address discovery
- [ ] `GetConnectedPeers()` — if kept, document: integration test assertions
- [ ] `Credentials()` — if kept, document: integration test auth setup

#### Internal methods:
- [ ] `RouteCall()` — verify unexported or internal interface only (wasm.InvocationRouter)

### Control server wiring
- [ ] Control server created INSIDE supervisor (New or Run), not externally
- [ ] `cmd/pln/main.go` only calls `supervisor.New()` and `supervisor.Run()` for node lifecycle
- [ ] Socket path passed to supervisor via config

### Extra types
- [ ] `supervisorTransportInfo` — unexported (lowercase)
- [ ] `supervisorMetricsSource` — unexported (lowercase)

### Extra package-level functions
- [ ] `GenIdentityKey` — moved out of supervisor or justified
- [ ] `ReadIdentityPub` — moved out of supervisor or justified

---

## Membership

### Constructor
- [ ] Signature: `New(self types.PeerKey, creds *auth.NodeCredentials, net Network, cluster ClusterState, opts ...Option) *Service`
- [ ] No more than 4 positional params + variadic options
- [ ] Former positional params moved to options: tracer, natDetector, nodeMetrics, signPriv, pollenDir, conf, log
- [ ] `PeerStore` interface removed (use `net.ConnectedPeers()` instead)

### Network interface
- [ ] `Connect(ctx, key, addrs) error` — present
- [ ] `Disconnect(key) error` — present (note: transport uses `Disconnect(key)` with no error return; interface should match transport)
- [ ] `PeerEvents() <-chan transport.PeerEvent` — present
- [ ] `ConnectedPeers() []types.PeerKey` — present
- [ ] `Send(ctx, peer, data) error` — present
- [ ] `Recv(ctx) (transport.Packet, error)` — present
- [ ] Extra methods beyond spec's 6 — each justified (cert renewal, stream opening, etc.)

### Exported methods on *Service
Spec methods (must be present):
- [ ] `Start(ctx context.Context) error`
- [ ] `Stop() error`
- [ ] `DenyPeer(key types.PeerKey) error`
- [ ] `Invite(subject string) (string, error)`
- [ ] `HandleClockStream(stream, peer)`
- [ ] `HandleCertRenewalStream(stream, peer)`
- [ ] `Events() <-chan state.Event`

Methods that MUST be unexported or removed:
- [ ] `Gossip` — unexported
- [ ] `BroadcastEvents` — unexported
- [ ] `BroadcastGossipBatches` — unexported
- [ ] `SendClockViaStream` — unexported
- [ ] `UpdateVivaldiCoords` — unexported
- [ ] `RefreshIPs` — unexported
- [ ] `SendObservedAddress` — unexported
- [ ] `HandleObservedAddress` — unexported
- [ ] `PublishCertExpiry` — unexported
- [ ] `CheckCertExpiry` — unexported
- [ ] `HandleCertRenewalRequest` — unexported
- [ ] `DisconnectExpiredPeers` — unexported
- [ ] `SetPeerConnectTime` — unexported
- [ ] `DeletePeerConnectTime` — unexported

Getters — each must be justified or removed:
- [ ] `LocalCoordValue()` — if kept, document caller
- [ ] `SmoothedErrValue()` — if kept, document caller
- [ ] `VivaldiSamplesCount()` — if kept, document caller
- [ ] `EagerSyncsCount()` — if kept, document caller
- [ ] `EagerSyncFailuresCount()` — if kept, document caller
- [ ] `IsRenewalFailed()` — if kept, document caller
- [ ] `RecordEagerSync()` — if kept, document caller
- [ ] `RecordEagerSyncFailure()` — if kept, document caller
- [ ] `SetSmoothedErr()` — removed or moved to test helper
- [ ] `SetNetwork()` — removed or moved to test helper

### Extra package-level functions
- [ ] `BuildTargetPeerSet` — unexported or moved into supervisor (only caller)
- [ ] `KnownPeersFromSnapshot` — unexported or moved into supervisor (only caller)

### Extra constants
- [ ] `MaxDatagramPayload` — unexported if only used within membership
- [ ] `CertCheckInterval` — unexported if only used within membership
- [ ] `CertWarnThreshold` — unexported if only used within membership
- [ ] `CertCriticalThreshold` — unexported if only used within membership
- [ ] `VivaldiWarmupDuration` — unexported if only used within membership
- [ ] `VivaldiErrAlpha` — unexported if only used within membership

### Extra interfaces
- [ ] `PeerStore` — removed (replaced by Network.ConnectedPeers)
- [ ] `Config` struct — absorbed into options or kept minimal

### Start() goroutines
- [ ] Gossip ticker — running
- [ ] Recv loop — running
- [ ] Peer event loop — running
- [ ] Peer tick (Vivaldi + disconnect expired) — running
- [ ] IP refresh ticker — running
- [ ] Cert check ticker — running
- [ ] Pending gossip broadcast — running
- [ ] All goroutines have `case <-ctx.Done()` branches

---

## Tunneling

### Constructor
- [ ] Signature matches spec: `New(self, services, streams, router, opts...) *Service`

### Exported methods on *Service
Spec methods (must be present):
- [ ] `Start(ctx) error`
- [ ] `Stop() error`
- [ ] `Connect(ctx, service, peer) error`
- [ ] `Disconnect(service) error`
- [ ] `ExposeService(port, name) error`
- [ ] `UnexposeService(name) error`
- [ ] `ListConnections() []ConnectionInfo`
- [ ] `HandleTunnelStream(stream, peer)`
- [ ] `HandleRoutedStream(stream, peer)`

Methods that MUST be unexported or removed:
- [ ] `SampleTraffic` — unexported (absorbed into Start)
- [ ] `ReconcileConnections` — unexported (absorbed into Start)
- [ ] `ReconcileDesiredConnections` — unexported (absorbed into Start)

Methods to evaluate — justify keeping or remove:
- [ ] `DisconnectPeer(peerID)` — if kept, document: cross-service PeerDenied coordination
- [ ] `RemoveDesiredConnectionsForPeer(pk)` — if kept, document: cross-service PeerDenied coordination
- [ ] `AddDesiredConnection(pk, remote, local)` — if kept, document caller; if only supervisor, consider removing
- [ ] `RemoveDesiredConnection(pk, remote, local)` — same
- [ ] `DesiredConnections()` — same

### Start() goroutines
- [ ] Traffic sampling ticker — running
- [ ] Reconciliation ticker — running
- [ ] All goroutines have `case <-ctx.Done()` branches

---

## Verification Gates

- [ ] `go build ./...` — passes
- [ ] `go test -count=1 ./...` — all unit tests pass
- [ ] `go test -tags integration -count=1 -timeout 120s ./pkg/integration/cluster/...` — passes
- [ ] `just lint` — 0 issues
- [ ] `cd infra && just verify-hetzner` — all 11 steps pass
