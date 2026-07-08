// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package static

import (
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

type parsedManifest struct {
	paths map[string][]byte
}

type manifestCache struct {
	m  map[string]*parsedManifest
	mu sync.RWMutex
}

func newManifestCache() *manifestCache { return &manifestCache{m: make(map[string]*parsedManifest)} }

func (c *manifestCache) load(digest string) (*parsedManifest, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pm, ok := c.m[digest]
	return pm, ok
}

func (c *manifestCache) store(digest string, pm *parsedManifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[digest] = pm
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	host := strings.ToLower(hostOnly(r.Host))
	snap := s.store.Snapshot()
	spec, ok := s.lookupSpec(snap, host)
	if !ok {
		http.NotFound(w, r)
		return
	}

	manifest, err := s.loadManifest(spec.Spec.ManifestDigest)
	if err != nil {
		s.log.Warnw("load manifest", "site", host, "err", err)
		http.Error(w, "site unavailable", http.StatusServiceUnavailable)
		return
	}

	reqPath := r.URL.Path
	if reqPath == "" || strings.HasSuffix(reqPath, "/") {
		reqPath += "index.html"
	}

	digest, ok := manifest.paths[reqPath]
	if !ok {
		// Log path-misses at debug only and emit the manifest's path
		// count instead of the full list; a probing scanner can
		// otherwise generate unbounded log volume against a public
		// listener.
		s.log.Debugw("static path miss", "host", host, "req_path", reqPath, "manifest_paths", len(manifest.paths))
		http.NotFound(w, r)
		return
	}
	hexDigest := hex.EncodeToString(digest)

	rc, err := s.blobs.Get(hexDigest)
	if err != nil {
		s.log.Warnw("blob get", "hash", types.ShortHash(hexDigest), "err", err)
		http.Error(w, "content unavailable", http.StatusServiceUnavailable)
		return
	}
	defer rc.Close()

	seeker, ok := rc.(io.ReadSeeker)
	if !ok {
		s.log.Warnw("blob not seekable", "hash", types.ShortHash(hexDigest))
		http.Error(w, "content unavailable", http.StatusServiceUnavailable)
		return
	}

	ctype := mime.TypeByExtension(filepath.Ext(reqPath))
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("ETag", `"`+hexDigest+`"`)
	http.ServeContent(w, r, reqPath, time.Time{}, seeker)
}

func (s *Service) loadManifest(digest string) (*parsedManifest, error) {
	if pm, ok := s.manifestCache.load(digest); ok {
		return pm, nil
	}
	rc, err := s.blobs.Get(digest)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(rc)
	rc.Close() //nolint:errcheck
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	m := &statev1.StaticManifest{}
	if err := m.UnmarshalVT(data); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}
	pm := &parsedManifest{paths: make(map[string][]byte, len(m.GetPaths()))}
	for _, p := range m.GetPaths() {
		pm.paths[p.GetPath()] = p.GetDigest()
	}
	s.manifestCache.store(digest, pm)
	return pm, nil
}

// lookupSpec resolves a Host header to a StaticSpec. With a configured
// domain the Host is `<name>-<slug>.<domain>` where slug is the
// publisher's PublisherSlug, so it carries the authority and resolves
// per-(authority,name) from the publication source. Without a domain
// the Host is a bare name with no authority channel: this mode is
// inherently single-tenant, and when two tenants name a site the same
// the deduped view's lowest-publisher tie-break wins. Multi-tenant
// static hosting requires a configured domain.
func (s *Service) lookupSpec(snap state.Snapshot, host string) (state.StaticSpecView, bool) {
	if s.domain == "" {
		spec, ok := snap.StaticSpecs[host]
		return spec, ok
	}
	if !strings.HasSuffix(host, s.domain) {
		return state.StaticSpecView{}, false
	}
	prefix := strings.TrimSuffix(host, s.domain)
	dash := strings.LastIndexByte(prefix, '-')
	if dash <= 0 {
		return state.StaticSpecView{}, false
	}
	name := prefix[:dash]
	slug := prefix[dash+1:]
	if !types.IsValidSlug(slug) {
		return state.StaticSpecView{}, false
	}
	for _, sv := range snap.StaticSpecsAll {
		if sv.Spec.Name == name && sv.Publisher.Slug() == slug {
			return sv, true
		}
	}
	return state.StaticSpecView{}, false
}

// hostOnly strips :port, handling IPv6 bracketed literals.
func hostOnly(hostport string) string {
	if hostport == "" {
		return ""
	}
	if hostport[0] == '[' {
		if end := strings.IndexByte(hostport, ']'); end > 0 {
			return hostport[1:end]
		}
		return hostport
	}
	if before, _, ok := strings.Cut(hostport, ":"); ok {
		return before
	}
	return hostport
}
