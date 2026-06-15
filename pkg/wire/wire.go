// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/transport"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// ProtocolMin and ProtocolMax bound the control-plane protocol versions
// this build speaks. Every node is re-bootstrapped together, so there is
// no legacy version to support; the range exists to give a forward,
// explicit answer when an old client later meets a new daemon (or the
// reverse).
const (
	ProtocolMin uint32 = 1
	ProtocolMax uint32 = 1
)

// clientIdentityTTL is short because the CLI mints a fresh identity
// cert on every connection.
const clientIdentityTTL = 5 * time.Minute

// ErrDaemonNoHandshake is returned when the daemon does not implement
// Handshake at all. A daemon that predates version negotiation is, by
// definition, too old to talk to a versioned client.
var ErrDaemonNoHandshake = errors.New("the daemon is out of date: it does not support protocol negotiation; upgrade the daemon")

// DaemonLacksHandshake reports whether err means the daemon has no
// Handshake RPC. It walks the whole error chain rather than reading only
// the outermost code, so a CodeUnimplemented rewrapped by an interceptor
// (which would otherwise surface as Unknown) is still recognised as a
// too-old daemon instead of being silently passed through.
func DaemonLacksHandshake(err error) bool {
	for err != nil {
		var ce *connect.Error
		if !errors.As(err, &ce) {
			return false
		}
		if ce.Code() == connect.CodeUnimplemented {
			return true
		}
		err = ce.Unwrap()
	}
	return false
}

// OutOfDateError is the explicit, typed result of a version mismatch.
// It carries both ranges so the CLI can tell the user exactly which
// side to upgrade and to what.
type OutOfDateError struct {
	ClientMin, ClientMax uint32
	ServerMin, ServerMax uint32
	clientBehind         bool
}

func (e *OutOfDateError) Error() string {
	if e.clientBehind {
		return fmt.Sprintf("pln is out of date: this daemon speaks protocol %d-%d but pln supports only up to %d; upgrade pln",
			e.ServerMin, e.ServerMax, e.ClientMax)
	}
	return fmt.Sprintf("the daemon is out of date: it speaks protocol %d-%d but pln requires at least %d; upgrade the daemon",
		e.ServerMin, e.ServerMax, e.ClientMin)
}

// CheckRange compares this build's [ProtocolMin, ProtocolMax] against
// the server range returned by Handshake. It returns nil when the
// ranges overlap and a typed *OutOfDateError otherwise.
func CheckRange(serverMin, serverMax uint32) error {
	if ProtocolMin <= serverMax && serverMin <= ProtocolMax {
		return nil
	}
	return &OutOfDateError{
		ClientMin:    ProtocolMin,
		ClientMax:    ProtocolMax,
		ServerMin:    serverMin,
		ServerMax:    serverMax,
		clientBehind: serverMin > ProtocolMax,
	}
}

// ClientTLSConfig builds the mTLS config the CLI dials the control
// plane with. It mints a fresh short-lived identity cert from the node
// credentials in dir. pln:// targets are not DNS-validated by Go's
// verifier; the pollen grant chain replaces SAN-based hostname checks,
// so VerifyPeerCertificate performs full chain plus leaf-key-binding
// verification instead.
func ClientTLSConfig(dir string) (*tls.Config, error) {
	identityDir := identity.IdentityPath(dir)
	creds, err := identity.LoadCredentials(identityDir)
	if err != nil {
		return nil, fmt.Errorf("load node credentials: %w", err)
	}
	if creds == nil || creds.Grant() == nil {
		return nil, errors.New("no node credentials in this context; run `pln join` first")
	}
	priv, _, err := identity.EnsureIdentityKey(identityDir)
	if err != nil {
		return nil, fmt.Errorf("load identity key: %w", err)
	}
	return ClientTLSConfigFromCreds(creds, priv)
}

// ClientTLSConfigFromCreds builds the same client TLS config from an
// in-memory credentials handle and signing key, for a long-lived
// process (the daemon) that already holds live credentials and must not
// re-read them from disk on every dial.
func ClientTLSConfigFromCreds(creds *identity.Credentials, signPriv ed25519.PrivateKey) (*tls.Config, error) {
	if creds == nil || creds.Grant() == nil {
		return nil, errors.New("no node credentials")
	}
	session, err := creds.EnsureFreshSession(time.Now(), clientIdentityTTL, clientIdentityTTL/2) //nolint:mnd
	if err != nil {
		return nil, fmt.Errorf("mint session: %w", err)
	}
	clientCert, err := transport.GenerateIdentityCert(signPriv, session, clientIdentityTTL)
	if err != nil {
		return nil, fmt.Errorf("generate client identity cert: %w", err)
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		Certificates:       []tls.Certificate{clientCert},
		InsecureSkipVerify: true, //nolint:gosec
		NextProtos:         []string{"h2"},
		// Client side has no cluster denylist; nil skips that check. The
		// server's grant chain + horizon are still enforced.
		VerifyPeerCertificate: transport.VerifyDelegatedCounterparty(creds.RootPub(), nil),
	}, nil
}

// renewRPCTimeout bounds a single RenewGrant dial+RPC. The daemon's
// grant-maintenance loop passes its long-lived context, so without a
// self-contained deadline a delegating peer that completes the handshake
// then stalls the RPC would wedge that loop (and with it the
// non-renewable expiry hard-stop it also drives).
const renewRPCTimeout = 30 * time.Second

// RenewGrantAt dials an admin-capable peer's control endpoint with the
// caller's own live credentials and asks it to re-mint the caller's
// grant. The server authenticates the caller from the mTLS session and
// enforces chain + denylist before re-issuing, so this is safe to call
// against any reachable delegating node, not only the original issuer.
func RenewGrantAt(ctx context.Context, addr string, creds *identity.Credentials, signPriv ed25519.PrivateKey) (*identityv1.Grant, error) {
	tlsCfg, err := ClientTLSConfigFromCreds(creds, signPriv)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, renewRPCTimeout)
	defer cancel()
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLS: func(network, a string, _ *tls.Config) (net.Conn, error) {
			return (&tls.Dialer{Config: tlsCfg}).DialContext(ctx, network, a)
		},
	}
	// The daemon calls this on every maintenance tick once renewal is due;
	// a GC'd http2.Transport does not reap its conns, so release them
	// rather than leaking a conn and its readLoop goroutine per attempt.
	defer tr.CloseIdleConnections()
	client := controlv1connect.NewControlServiceClient(&http.Client{Transport: tr}, "https://"+addr, connect.WithGRPC())
	resp, err := client.RenewGrant(ctx, connect.NewRequest(&controlv1.RenewGrantRequest{}))
	if err != nil {
		return nil, fmt.Errorf("renew grant rpc: %w", err)
	}
	return resp.Msg.GetGrant(), nil
}

// MaybeRenewGrant opportunistically renews a wire-mode tenant's grant
// when it is within the renewal lead window, against the control
// endpoint at addr, persisting the result so the next dial uses it. A
// wire-mode tenant has no daemon to run the proactive loop, so this is
// the renewal path for `pln join` clients. Best-effort: it returns an
// error only when a renewal was due and failed; the caller proceeds on
// the current still-valid grant (renewal runs well before the deadline)
// and retries on the next invocation. It is a no-op when this context
// is not enrolled or the grant is not yet due.
func MaybeRenewGrant(ctx context.Context, dir, addr string) error {
	identityDir := identity.IdentityPath(dir)
	creds, err := identity.LoadCredentials(identityDir)
	if errors.Is(err, identity.ErrCredentialsNotFound) {
		// Not enrolled: a fresh `pln join` runs this hook before its
		// own body writes credentials. Nothing to renew, not an error.
		return nil
	}
	if err != nil {
		return fmt.Errorf("load node credentials: %w", err)
	}
	if !identity.GrantRenewDue(creds.Grant(), time.Now()) {
		return nil
	}
	priv, _, err := identity.EnsureIdentityKey(identityDir)
	if err != nil {
		return fmt.Errorf("load identity key: %w", err)
	}
	g, err := RenewGrantAt(ctx, addr, creds, priv)
	if err != nil {
		return err
	}
	// nil denylist: the wire client holds no cluster snapshot, and the
	// issuing server already enforced the denylist before re-issuing.
	return creds.AdoptGrant(g, time.Now(), nil)
}

// ServerCertProvider mints and caches the control-plane server identity
// cert, re-minting it before the embedded Session expires. The control TLS
// listener lives for the whole daemon, so a static cert would present an
// expired Session after one TTL and break every wire client, including the
// daemon-to-daemon grant-renewal path that dials this listener.
// GetCertificate is safe for concurrent use by crypto/tls.
type ServerCertProvider struct {
	notAfter time.Time
	creds    *identity.Credentials
	now      func() time.Time
	cert     *tls.Certificate
	signPriv ed25519.PrivateKey
	ttl      time.Duration
	mu       sync.Mutex
}

func NewServerCertProvider(creds *identity.Credentials, signPriv ed25519.PrivateKey, ttl time.Duration) *ServerCertProvider {
	return &ServerCertProvider{creds: creds, signPriv: signPriv, ttl: ttl, now: time.Now}
}

// GetCertificate returns a live server cert, minting a fresh Session and
// leaf once the cached one is within half its TTL of expiry. The half-TTL
// margin matches EnsureFreshSession, so leaf and Session rotate together.
func (p *ServerCertProvider) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cert != nil && now.Add(p.ttl/2).Before(p.notAfter) { //nolint:mnd
		return p.cert, nil
	}
	session, err := p.creds.EnsureFreshSession(now, p.ttl, p.ttl/2) //nolint:mnd
	if err != nil {
		return nil, fmt.Errorf("control tls session: %w", err)
	}
	cert, err := transport.GenerateIdentityCert(p.signPriv, session, p.ttl)
	if err != nil {
		return nil, fmt.Errorf("control tls identity cert: %w", err)
	}
	p.cert = &cert
	p.notAfter = now.Add(p.ttl)
	return p.cert, nil
}

// ServerTLSConfig builds the TLS config for the control RPC listener.
// getCertificate supplies the server leaf per handshake (see
// ServerCertProvider) so a long-lived listener keeps presenting a live
// Session. Inbound clients must present a cert whose Session extension
// chains back to the configured root AND whose TLS leaf public key matches
// the Session's grant subject: without that binding, anyone who has seen
// the victim's gossiped Session can mint a new leaf and impersonate them.
// The verified Session is later retrieved from the gRPC peer context by
// CallerGrantFromContext.
func ServerTLSConfig(getCertificate func(*tls.ClientHelloInfo) (*tls.Certificate, error), rootPub []byte, denied identity.DenyChecker) *tls.Config {
	return &tls.Config{
		MinVersion:            tls.VersionTLS13,
		GetCertificate:        getCertificate,
		ClientAuth:            tls.RequireAnyClientCert,
		NextProtos:            []string{"h2"},
		VerifyPeerCertificate: transport.VerifyDelegatedCounterparty(rootPub, denied),
	}
}

// CallerGrantFromContext returns the verified caller Grant if the
// inbound gRPC session carried an mTLS peer cert with our pollen
// Session extension. Returns nil for unix-socket and SSH-bridge
// transports, which carry no peer cert.
func CallerGrantFromContext(ctx context.Context) *identityv1.Grant {
	return callerSessionFromContext(ctx).GetClaims().GetGrant()
}

// CallerCredentialFromContext returns the caller's grant together with
// the session's grant-subject proof-of-possession. The serving node
// relays this pair into cluster state so a daemonless wire publisher's
// grant clears the same isAcceptableGrantEvent gate a gossiped grant
// does. Returns (nil, nil) for transports that carry no peer cert.
func CallerCredentialFromContext(ctx context.Context) (*identityv1.Grant, []byte) {
	session := callerSessionFromContext(ctx)
	return session.GetClaims().GetGrant(), session.GetSubjectSignature()
}

func callerSessionFromContext(ctx context.Context) *identityv1.Session {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return nil
	}
	leaf := tlsInfo.State.PeerCertificates[0]
	session, err := transport.ParseSessionExtension(leaf.Raw)
	if err != nil {
		return nil
	}
	return session
}
