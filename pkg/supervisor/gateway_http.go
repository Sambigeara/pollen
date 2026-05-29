// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	gatewayReadHeaderTimeout = 10 * time.Second
	// gatewayReadTimeout bounds the whole request read (headers plus body)
	// so a slow client cannot dribble a body and pin a handler goroutine.
	// The body is separately capped at placement.MaxInputLen, so this only
	// needs to be generous enough for a legitimate slow upload. No
	// WriteTimeout is set: the gateway streams arbitrarily large blobs and
	// allows long-running workload invocations, so a fixed write deadline
	// would sever legitimate responses.
	gatewayReadTimeout = 60 * time.Second
	gatewayIdleTimeout = 120 * time.Second
	subdomainBlob      = "blob"
	subdomainFn        = "fn"

	// Per-token rate limit for the anonymous HTTP gateway. AccessToken is
	// a bearer credential with a TTL; without a per-token throttle a
	// leaked URL becomes a free amplification channel against the
	// workload runtime. The bucket size accommodates legitimate burst
	// (page reload, retry storms) while the refill rate caps sustained
	// abuse.
	gatewayTokenRPS        = 10
	gatewayTokenBurst      = 20
	gatewayLimiterCapacity = 10000
)

func (n *Supervisor) startGatewayHTTP(ctx context.Context, addr string) error {
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("gateway http listen: %w", err)
	}
	n.log.Infow("gateway http listener", "addr", l.Addr().String())
	srv := &http.Server{
		Handler:           newGatewayHandler(n.gate, n.store, n.blobs, n.placement, n.log),
		ReadHeaderTimeout: gatewayReadHeaderTimeout,
		ReadTimeout:       gatewayReadTimeout,
		IdleTimeout:       gatewayIdleTimeout,
	}
	// Track the shutdown watcher via the supervisor's WaitGroup so
	// Run() returns only after both Serve and its closer have
	// finished; bare goroutines outlive the supervisor's context and
	// can panic on closed channels during teardown.
	n.spawn(func() {
		<-ctx.Done()
		srv.Close() //nolint:errcheck
	})
	if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type gatewayBlobReader interface {
	FetchPlaintext(ctx context.Context, hash string) (io.ReadCloser, error)
}

type gatewayWorkloadInvoker interface {
	Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
}

type gatewayHandler struct {
	gate      *admission.Pipeline
	snap      admission.StateReader
	blobs     gatewayBlobReader
	placement gatewayWorkloadInvoker
	log       *zap.SugaredLogger
	limiter   *tokenLimiter
}

func newGatewayHandler(g *admission.Pipeline, snap admission.StateReader, b gatewayBlobReader, p gatewayWorkloadInvoker, log *zap.SugaredLogger) *gatewayHandler {
	return &gatewayHandler{
		gate:      g,
		snap:      snap,
		blobs:     b,
		placement: p,
		log:       log,
		limiter:   newTokenLimiter(gatewayTokenRPS, gatewayTokenBurst, gatewayLimiterCapacity),
	}
}

// tokenLimiter is a per-token token-bucket rate limiter with bounded
// memory. Entries are pruned in insertion order (FIFO) when the cache
// fills up. Callers MUST key on a validated identity (e.g. the
// verified issuer pub plus resource ID): keying on the raw URL path
// would let an attacker churn the cache with garbage paths and evict
// legitimate buckets.
type tokenLimiter struct {
	buckets  map[[32]byte]*tokenBucket
	now      func() time.Time
	order    []bucketKey
	rps      float64
	burst    float64
	capacity int
	mu       sync.Mutex
}

type tokenBucket struct {
	last   time.Time
	tokens float64
}

type bucketKey [32]byte

func newTokenLimiter(rps, burst, capacity int) *tokenLimiter { //nolint:unparam
	return &tokenLimiter{
		buckets:  make(map[[32]byte]*tokenBucket),
		rps:      float64(rps),
		burst:    float64(burst),
		capacity: capacity,
		now:      time.Now,
	}
}

// allow returns true and consumes one token from the bucket keyed on
// the supplied identity. When the bucket is empty it returns false
// plus the duration the caller should wait before retrying.
func (l *tokenLimiter) allow(key [32]byte) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	bucket, ok := l.buckets[key]
	if !ok {
		bucket = &tokenBucket{tokens: l.burst, last: now}
		l.buckets[key] = bucket
		l.order = append(l.order, key)
		l.evictIfFullLocked()
	}
	elapsed := now.Sub(bucket.last).Seconds()
	bucket.tokens = min(l.burst, bucket.tokens+elapsed*l.rps)
	bucket.last = now
	if bucket.tokens < 1 {
		retry := time.Duration((1-bucket.tokens)/l.rps*float64(time.Second)) + time.Millisecond
		return false, retry
	}
	bucket.tokens--
	return true, 0
}

// tokenLimiterKey derives a stable per-token identity from the
// signature-verified claims. Using (issuer_pub || resource_id_bytes)
// makes the key independent of payload-encoding noise and binds a
// caller's rate budget to the resource they're invoking: two
// concurrent shares from one issuer over different resources get
// separate buckets.
func tokenLimiterKey(token *admissionv1.AccessToken) [32]byte {
	claims := token.GetClaims()
	hasher := sha256.New()
	hasher.Write(claims.GetIssuerPub())
	if res := claims.GetResource(); res != nil {
		// Deterministic marshal so two hits on the same token always
		// land in the same bucket. Standard proto.Marshal is allowed
		// to vary across calls (notably with maps); the rest of the
		// codebase already standardises on the deterministic option
		// at signing time.
		if b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(res); err == nil {
			hasher.Write(b)
		}
	}
	var key [32]byte
	copy(key[:], hasher.Sum(nil))
	return key
}

func (l *tokenLimiter) evictIfFullLocked() {
	if len(l.buckets) <= l.capacity {
		return
	}
	// Drop the oldest insertion. Keys observed since the cache filled
	// up will re-fill on next hit at l.burst tokens, fine for the
	// "leaked URL" attack model since the new entry still has to
	// refill from one per RPS.
	victim := l.order[0]
	l.order = l.order[1:]
	delete(l.buckets, victim)
}

func (h *gatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host, _, splitErr := net.SplitHostPort(r.Host)
	if splitErr != nil {
		host = r.Host
	}
	subdomain, _, _ := strings.Cut(strings.ToLower(host), ".")
	if subdomain != subdomainBlob && subdomain != subdomainFn {
		http.Error(w, "unknown gateway", http.StatusNotFound)
		return
	}
	first, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	switch first {
	case "":
		http.Error(w, "missing route", http.StatusBadRequest)
	case types.ReservedBearerSlug:
		h.serveBearer(w, r, subdomain, rest)
	default:
		h.serveNamed(w, r, subdomain, first, rest)
	}
}

// serveNamed handles canonical URLs of the form
// `<sub>.<gw>/<publisher-slug>/<resource>[/<fn>]`. Authorisation runs
// against the spec's policy with no caller cert; only specs with
// public=true admit anonymous callers.
func (h *gatewayHandler) serveNamed(w http.ResponseWriter, r *http.Request, subdomain, slug, rest string) {
	if !types.IsValidSlug(slug) {
		http.NotFound(w, r)
		return
	}
	switch subdomain {
	case subdomainBlob:
		h.handleNamedFetch(w, r, slug, rest)
	case subdomainFn:
		h.handleNamedInvoke(w, r, slug, rest)
	}
}

func (h *gatewayHandler) handleNamedFetch(w http.ResponseWriter, r *http.Request, slug, name string) {
	if name == "" || strings.Contains(name, "/") {
		http.NotFound(w, r)
		return
	}
	hash, f, ok := h.resolveBlob(slug, name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if err := h.gate.AllowAnonymous(f); err != nil {
		http.NotFound(w, r)
		return
	}
	h.streamBlobBytes(w, r, hash, name)
}

func (h *gatewayHandler) handleNamedInvoke(w http.ResponseWriter, r *http.Request, slug, rest string) {
	name, fn, _ := strings.Cut(rest, "/")
	if name == "" || strings.Contains(fn, "/") {
		http.NotFound(w, r)
		return
	}
	if fn == "" {
		fn = "main"
	}
	hash, f, authority, ok := h.resolveWorkload(slug, name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if err := h.gate.AllowAnonymous(f); err != nil {
		http.NotFound(w, r)
		return
	}
	// Carry the resolved (authority, name) onto the dispatch hop so a
	// remote edge that claims the workload authorises this exact
	// publication, not whichever co-publisher of identical bytes wins
	// the deduped artefact view.
	r = r.WithContext(admission.WithInvokedPublication(r.Context(), &admission.Publication{AuthorityPub: authority.Bytes(), Name: name}))
	h.callWorkload(w, r, hash, fn)
}

// resolveBlob and resolveWorkload map a public `<slug>/<name>` URL to a
// fact. The slug is the publisher's one-way PublisherSlug, so it is the
// authority selector: iterate the per-(authority,name) publication
// source, not the deduped runtime map, or a tenant whose bytes collide
// with another's would be unreachable by its own URL.
func (h *gatewayHandler) resolveBlob(slug, name string) (string, *factv1.Fact, bool) {
	for _, sv := range h.snap.Snapshot().BlobSpecsAll {
		if sv.Publisher.Slug() == slug && sv.Spec.Name == name {
			return sv.Spec.Digest, sv.Fact, true
		}
	}
	return "", nil, false
}

func (h *gatewayHandler) resolveWorkload(slug, name string) (string, *factv1.Fact, types.PeerKey, bool) {
	for _, sv := range h.snap.Snapshot().SpecsAll {
		if sv.Publisher.Slug() == slug && sv.Spec.Name == name {
			return sv.Spec.Hash, sv.Fact, sv.Publisher, true
		}
	}
	return "", nil, types.PeerKey{}, false
}

// serveBearer handles `/_/<token>` URLs minted by `pln share`. The token
// is the entire credential: decoded, signature-verified, and rate-limited
// before any dispatch so an attacker can't pollute the limiter cache by
// hitting garbage paths.
func (h *gatewayHandler) serveBearer(w http.ResponseWriter, r *http.Request, subdomain, encoded string) {
	if encoded == "" {
		http.Error(w, "missing token", http.StatusBadRequest)
		return
	}
	token, err := auth.DecodeAccessToken(encoded)
	if err != nil {
		http.Error(w, "invalid token", http.StatusForbidden)
		return
	}
	now := time.Now()
	if expired(token, now) {
		http.Error(w, "token expired", http.StatusGone)
		return
	}
	if err := auth.VerifyAccessToken(token, now); err != nil {
		http.Error(w, "token verify failed", http.StatusForbidden)
		return
	}
	if allowed, retryAfter := h.limiter.allow(tokenLimiterKey(token)); !allowed {
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())+1))
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	switch subdomain {
	case subdomainBlob:
		h.handleFetch(w, r, token)
	case subdomainFn:
		h.handleInvoke(w, r, token)
	}
}

func (h *gatewayHandler) handleFetch(w http.ResponseWriter, r *http.Request, token *admissionv1.AccessToken) {
	blobClaim := token.GetClaims().GetResource().GetBlob()
	if blobClaim == nil {
		http.Error(w, "blob gateway requires a blob token", http.StatusForbidden)
		return
	}
	hash := hex.EncodeToString(blobClaim.GetDigest())
	if err := h.gate.FetchByToken(token, hash); err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	h.streamBlobBytes(w, r, hash, blobClaim.GetName())
}

func (h *gatewayHandler) handleInvoke(w http.ResponseWriter, r *http.Request, token *admissionv1.AccessToken) {
	seedClaim := token.GetClaims().GetResource().GetSeed()
	if seedClaim == nil {
		http.Error(w, "fn gateway requires a workload token", http.StatusForbidden)
		return
	}
	hash := hex.EncodeToString(seedClaim.GetHash())
	if _, err := h.gate.InvokeByToken(token, hash); err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	fn := r.URL.Query().Get("fn")
	if fn == "" {
		fn = "main"
	}
	// Carry the token onto the dispatch hop so any remote edge that
	// claims the workload authorises by token end-to-end.
	r = r.WithContext(admission.WithAccessToken(r.Context(), token))
	h.callWorkload(w, r, hash, fn)
}

func (h *gatewayHandler) streamBlobBytes(w http.ResponseWriter, r *http.Request, hash, filename string) {
	rc, err := h.blobs.FetchPlaintext(r.Context(), hash)
	if err != nil {
		h.log.Warnw("gateway fetch failed", "hash", hash, "err", err)
		http.Error(w, "fetch failed", http.StatusServiceUnavailable)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if disp := mime.FormatMediaType("attachment", map[string]string{"filename": filename}); disp != "" {
		w.Header().Set("Content-Disposition", disp)
	}
	if _, err := io.Copy(w, rc); err != nil {
		h.log.Warnw("gateway stream error", "hash", hash, "err", err)
	}
}

func (h *gatewayHandler) callWorkload(w http.ResponseWriter, r *http.Request, hash, fn string) {
	// Cap ingress at the wire limit before buffering, so an anonymous
	// caller cannot force an unbounded allocation: the named (public) path
	// has no token-bucket throttle in front of it.
	r.Body = http.MaxBytesReader(w, r.Body, placement.MaxInputLen)
	input, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	output, err := h.placement.Call(r.Context(), hash, fn, input)
	if err != nil {
		h.log.Warnw("gateway invoke failed", "hash", hash, "function", fn, "err", err)
		http.Error(w, "invoke failed", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, writeErr := w.Write(output); writeErr != nil { //nolint:gosec
		h.log.Warnw("gateway write error", "hash", hash, "err", writeErr)
	}
}

func expired(token *admissionv1.AccessToken, now time.Time) bool {
	claims := token.GetClaims()
	if claims == nil {
		return true
	}
	return now.Unix() > claims.GetExpiresAtUnix()
}
