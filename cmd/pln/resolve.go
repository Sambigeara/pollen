package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/types"
)

// errNoSuffixMatch is returned by suffix-fallback resolvers when no service
// name prefix matches the argument. Callers use it to distinguish "no match"
// from a collision error that should be propagated to the user.
var errNoSuffixMatch = errors.New("no suffix match")

func resolveConnection(st *controlv1.GetStatusResponse, arg, providerArg string) (*controlv1.ConnectionSummary, error) {
	var matches []*controlv1.ConnectionSummary

	if providerArg == "" && isPortArg(arg) {
		p, _ := strconv.Atoi(arg)
		localPort := uint32(p)
		for _, c := range st.GetConnections() {
			if c.GetLocalPort() == localPort {
				matches = append(matches, c)
			}
		}
	}

	if len(matches) == 0 {
		for _, c := range st.GetConnections() {
			if c.GetServiceName() == arg {
				if providerArg != "" && !peerIDHasPrefix(c.GetPeer().GetPeerPub(), providerArg) {
					continue
				}
				matches = append(matches, c)
			}
		}
	}

	if len(matches) == 1 {
		return matches[0], nil
	}

	if len(matches) > 1 {
		return nil, connectionCollisionError(arg, matches)
	}

	// No exact match — try suffix fallback (e.g. "http-ab" → name "http", prefix "ab").
	if providerArg == "" && !isPortArg(arg) {
		if c, err := resolveBySuffix(arg, st.GetConnections(),
			func(c *controlv1.ConnectionSummary) suffixCandidate {
				return suffixCandidate{
					name:    c.GetServiceName(),
					peerKey: peerKeyString(c.GetPeer().GetPeerPub()),
					include: true,
				}
			},
			connectionCollisionError,
		); !errors.Is(err, errNoSuffixMatch) {
			return c, err
		}
	}

	return nil, fmt.Errorf("no active connection matching %q — run \"pln status\" to see active connections", arg)
}

func connectionCollisionError(name string, matches []*controlv1.ConnectionSummary) error {
	ids := make([]string, len(matches))
	for i, c := range matches {
		ids[i] = peerKeyString(c.GetPeer().GetPeerPub())
	}
	prefixes := minUniquePrefixes(ids)

	var b strings.Builder
	fmt.Fprintf(&b, "multiple connections match %q — pick one:\n", name)
	for i, c := range matches {
		provider := formatPeerID(c.GetPeer().GetPeerPub(), false)
		connName := c.GetServiceName()
		if connName == "" {
			connName = strconv.FormatUint(uint64(c.GetRemotePort()), 10)
		}
		fmt.Fprintf(&b, "  pln disconnect %s-%s    (localhost:%d -> %s:%d)\n",
			connName, prefixes[i], c.GetLocalPort(), provider, c.GetRemotePort())
	}
	return errors.New(strings.TrimSpace(b.String()))
}

func reachableProviderSet(st *controlv1.GetStatusResponse) map[string]bool {
	m := map[string]bool{}
	if st.GetSelf() != nil && st.GetSelf().GetNode() != nil {
		m[peerKeyString(st.GetSelf().GetNode().GetPeerPub())] = true
	}
	for _, n := range st.Nodes {
		if isReachableStatus(n.GetStatus()) {
			m[peerKeyString(n.GetNode().GetPeerPub())] = true
		}
	}
	return m
}

func resolveService(st *controlv1.GetStatusResponse, serviceArg, providerArg string) (*controlv1.ServiceSummary, error) {
	reachableProviders := reachableProviderSet(st)

	portFilter := uint32(0)
	if p, err := strconv.Atoi(serviceArg); err == nil && p >= minPort && p <= maxPort {
		portFilter = uint32(p)
	}

	matches := make([]*controlv1.ServiceSummary, 0, len(st.Services))
	for _, svc := range st.Services {
		if portFilter > 0 {
			if svc.GetPort() != portFilter && svc.GetName() != serviceArg {
				continue
			}
		} else if svc.GetName() != serviceArg {
			continue
		}

		providerID := peerKeyString(svc.GetProvider().GetPeerPub())
		if !reachableProviders[providerID] {
			continue
		}
		if providerArg != "" && !peerIDHasPrefix(svc.GetProvider().GetPeerPub(), providerArg) {
			continue
		}
		matches = append(matches, svc)
	}

	if len(matches) == 1 {
		return matches[0], nil
	}

	if len(matches) > 1 {
		return nil, serviceCollisionError(serviceArg, matches)
	}

	// No exact match — try suffix fallback (e.g. "http-ab" → name "http", prefix "ab").
	if providerArg == "" && portFilter == 0 {
		if svc, err := resolveBySuffix(serviceArg, st.Services,
			func(svc *controlv1.ServiceSummary) suffixCandidate {
				pk := peerKeyString(svc.GetProvider().GetPeerPub())
				return suffixCandidate{name: svc.GetName(), peerKey: pk, include: reachableProviders[pk]}
			},
			serviceCollisionError,
		); !errors.Is(err, errNoSuffixMatch) {
			return svc, err
		}
	}

	if providerArg != "" {
		return nil, fmt.Errorf("no reachable provider for %q on %q — run \"pln status\" to see available services", serviceArg, providerArg)
	}
	return nil, fmt.Errorf("no reachable service matching %q — run \"pln status\" to see available services", serviceArg)
}

func serviceCollisionError(name string, matches []*controlv1.ServiceSummary) error {
	ids := make([]string, len(matches))
	for i, svc := range matches {
		ids[i] = peerKeyString(svc.GetProvider().GetPeerPub())
	}
	prefixes := minUniquePrefixes(ids)

	var b strings.Builder
	fmt.Fprintf(&b, "multiple services match %q — pick one:\n", name)
	for i, svc := range matches {
		provider := formatPeerID(svc.GetProvider().GetPeerPub(), false)
		fmt.Fprintf(&b, "  pln connect %s-%s    (%s:%d)\n", name, prefixes[i], provider, svc.GetPort())
	}
	return errors.New(strings.TrimSpace(b.String()))
}

type suffixCandidate struct {
	name    string
	peerKey string
	include bool // false → skip (e.g. unreachable); connections set this to true
}

// resolveBySuffix attempts suffix-based resolution (e.g. "http-ab" → name
// "http", provider prefix "ab") over a list of items. Returns errNoSuffixMatch
// when no name matches the argument.
func resolveBySuffix[T any](arg string, items []T, toCandidate func(T) suffixCandidate, collisionErr func(string, []T) error) (T, error) {
	names := map[string]bool{}
	candidates := make([]suffixCandidate, 0, len(items))
	for _, item := range items {
		c := toCandidate(item)
		if c.name != "" {
			names[c.name] = true
		}
		candidates = append(candidates, c)
	}

	indices, name, err := matchSuffixCandidates(arg, names, candidates)
	if err != nil {
		var zero T
		return zero, err
	}

	matches := make([]T, len(indices))
	for i, idx := range indices {
		matches[i] = items[idx]
	}
	if len(matches) > 1 {
		var zero T
		return zero, collisionErr(name, matches)
	}
	return matches[0], nil
}

// matchSuffixCandidates parses a "name-prefix" arg, then returns the indices
// of candidates that match. Returns errNoSuffixMatch when no name matches or
// no candidates match the prefix.
func matchSuffixCandidates(arg string, knownNames map[string]bool, items []suffixCandidate) ([]int, string, error) {
	name, prefix, ok := parseSuffixedArg(arg, knownNames)
	if !ok {
		return nil, "", errNoSuffixMatch
	}

	var indices []int
	for i, item := range items {
		if item.name != name {
			continue
		}
		if !item.include {
			continue
		}
		if strings.HasPrefix(item.peerKey, prefix) {
			indices = append(indices, i)
		}
	}
	if len(indices) == 0 {
		return nil, name, errNoSuffixMatch
	}
	return indices, name, nil
}

// parseSuffixedArg parses "name-prefix" against known service names,
// returning the longest matching name and the provider prefix after the dash.
func parseSuffixedArg(arg string, knownNames map[string]bool) (name, prefix string, ok bool) {
	for n := range knownNames {
		if !strings.HasPrefix(arg, n+"-") {
			continue
		}
		p := arg[len(n)+1:]
		if p == "" {
			continue
		}
		if len(n) > len(name) {
			name = n
			prefix = p
			ok = true
		}
	}
	return name, prefix, ok
}

func peerKeyString(peerID []byte) string {
	if len(peerID) == 0 {
		return ""
	}
	return types.PeerKeyFromBytes(peerID).String()
}

func formatPeerID(peerID []byte, wide bool) string {
	full := peerKeyString(peerID)
	if full == "" {
		return "-"
	}
	if wide || len(full) <= 8 {
		return full
	}
	return full[:8]
}

func formatStatus(s controlv1.NodeStatus) string {
	switch s {
	case controlv1.NodeStatus_NODE_STATUS_ONLINE:
		return "direct"
	case controlv1.NodeStatus_NODE_STATUS_INDIRECT:
		return "indirect"
	default:
		return "offline"
	}
}

func isReachableStatus(s controlv1.NodeStatus) bool {
	return s == controlv1.NodeStatus_NODE_STATUS_ONLINE ||
		s == controlv1.NodeStatus_NODE_STATUS_INDIRECT
}

func peerIDHasPrefix(peerID []byte, prefix string) bool {
	return strings.HasPrefix(peerKeyString(peerID), strings.ToLower(prefix))
}

// minUniquePrefixes returns the shortest prefix of each string that
// distinguishes it from all other strings in the set.
func minUniquePrefixes(ids []string) []string {
	n := len(ids)
	out := make([]string, n)
	for i, id := range ids {
		prefixLen := 1
		for j, other := range ids {
			if i == j {
				continue
			}
			common := 0
			for common < len(id) && common < len(other) && id[common] == other[common] {
				common++
			}
			if common+1 > prefixLen {
				prefixLen = common + 1
			}
		}
		if prefixLen > len(id) {
			prefixLen = len(id)
		}
		out[i] = id[:prefixLen]
	}
	return out
}

type serviceProviderKey struct {
	name     string
	provider string
}

// serviceNameSuffixes computes minimal unique suffixes for services with
// colliding names. Non-colliding names have no entry.
func serviceNameSuffixes(services []*controlv1.ServiceSummary, include func(string) bool) map[serviceProviderKey]string {
	groups := map[string][]string{}
	for _, svc := range services {
		pk := peerKeyString(svc.GetProvider().GetPeerPub())
		if !include(pk) {
			continue
		}
		groups[svc.GetName()] = append(groups[svc.GetName()], pk)
	}

	result := map[serviceProviderKey]string{}
	for name, pks := range groups {
		if len(pks) < 2 { //nolint:mnd
			continue
		}
		prefixes := minUniquePrefixes(pks)
		for i, pk := range pks {
			result[serviceProviderKey{name, pk}] = prefixes[i]
		}
	}
	return result
}

func resolveServiceByPrefix(services []config.Service, prefix string) (string, error) {
	var matches []string
	for _, s := range services {
		if strings.HasPrefix(s.Name, prefix) {
			matches = append(matches, s.Name)
		}
	}

	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no service matching %q", prefix)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("prefix %q matches multiple services: %s", prefix, strings.Join(matches, ", "))
	}
}

func isPortArg(s string) bool {
	p, err := strconv.Atoi(s)
	return err == nil && p >= minPort && p <= maxPort
}
