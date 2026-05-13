// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/gate"
)

const (
	gatewayReadHeaderTimeout = 10 * time.Second
	subdomainBlob            = "blob"
	subdomainFn              = "fn"
)

func (n *Supervisor) startGatewayHTTP(ctx context.Context, addr string) error {
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("gateway http listen: %w", err)
	}
	n.log.Infow("gateway http listener", "addr", l.Addr().String())
	srv := &http.Server{
		Handler:           newGatewayHandler(n.gate, n.blobs, n.placement, n.log),
		ReadHeaderTimeout: gatewayReadHeaderTimeout,
	}
	go func() {
		<-ctx.Done()
		srv.Close() //nolint:errcheck
	}()
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
	gate      *gate.Gate
	blobs     gatewayBlobReader
	placement gatewayWorkloadInvoker
	log       *zap.SugaredLogger
}

func newGatewayHandler(g *gate.Gate, b gatewayBlobReader, p gatewayWorkloadInvoker, log *zap.SugaredLogger) *gatewayHandler {
	return &gatewayHandler{gate: g, blobs: b, placement: p, log: log}
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
	encoded := strings.TrimPrefix(r.URL.Path, "/")
	if encoded == "" {
		http.Error(w, "missing token", http.StatusBadRequest)
		return
	}
	token, err := auth.DecodeAccessToken(encoded)
	if err != nil {
		http.Error(w, "invalid token", http.StatusForbidden)
		return
	}
	if expired(token, time.Now()) {
		http.Error(w, "token expired", http.StatusGone)
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
	rc, err := h.blobs.FetchPlaintext(r.Context(), hash)
	if err != nil {
		h.log.Warnw("gateway fetch failed", "hash", hash, "err", err)
		http.Error(w, "fetch failed", http.StatusServiceUnavailable)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, err := io.Copy(w, rc); err != nil {
		h.log.Warnw("gateway stream error", "hash", hash, "err", err)
	}
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
	function := r.URL.Query().Get("fn")
	if function == "" {
		function = "main"
	}
	input, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	// Hand the token to placement so the dispatch hop authorises by
	// token (not peer cert) end-to-end, including any remote edge that
	// claims the workload.
	ctx := gate.WithAccessToken(r.Context(), token)
	output, err := h.placement.Call(ctx, hash, function, input)
	if err != nil {
		h.log.Warnw("gateway invoke failed", "hash", hash, "function", function, "err", err)
		http.Error(w, "invoke failed", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, writeErr := w.Write(output); writeErr != nil { //nolint:gosec // opaque binary payload; nosniff above blocks browser sniffing
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
