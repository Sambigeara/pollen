package link

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"net"
	"strconv"
	"time"
)

type birthdayPunchState struct {
	host      string
	basePorts []int // Ports observed by coordinator
	portMin   int
	portMax   int
	rand      *rand.Rand
	seen      map[int]struct{}
	attempts  int
}

func newBirthdayPunchState(cfg config, addrs []string) *birthdayPunchState {
	if !cfg.birthdayPunchEnabled || cfg.birthdayPunchPortsPerAttempt <= 0 {
		return nil
	}
	if cfg.birthdayPunchPortMin <= 0 || cfg.birthdayPunchPortMax <= 0 || cfg.birthdayPunchPortMin > cfg.birthdayPunchPortMax {
		return nil
	}

	host, basePorts := pickBirthdayHost(addrs)
	if host == "" {
		return nil
	}

	seen := make(map[int]struct{}, len(basePorts))
	for _, p := range basePorts {
		seen[p] = struct{}{}
	}

	return &birthdayPunchState{
		host:      host,
		basePorts: basePorts,
		portMin:   cfg.birthdayPunchPortMin,
		portMax:   cfg.birthdayPunchPortMax,
		rand:      newBirthdayRand(cfg),
		seen:      seen,
		attempts:  0,
	}
}

func (s *birthdayPunchState) nextAddrs(count int) []string {
	if s == nil || count <= 0 {
		return nil
	}

	ports := make([]int, 0, count)

	// 1. Proximity Spray: Target ports near the base ports (heuristic for symmetric NATs).
	// Crucial: We ignore s.portMin/Max here. If we observe port 5000, we must scan near 5000,
	// even if our config says "ephemeral ports only" (49152+).
	for _, base := range s.basePorts {
		// Divide the quota among base ports
		quota := count / len(s.basePorts)
		if quota < 1 {
			quota = 1
		}

		for i := 0; i < quota; i++ {
			// Generate offsets: 0, 1, -1, 2, -2...
			offset := 0
			if i%2 == 0 {
				offset = i / 2
			} else {
				offset = -(i/2 + 1)
			}

			// Add slight jitter to avoid exact lockstep
			jitter := s.rand.Intn(10) - 5
			p := base + offset + jitter

			// Use hard limits (1-65535) for proximity
			if s.addPort(p, &ports, count, 1, 65535) {
				goto Done
			}
		}
	}

	// 2. Random Spray: Fill remainder with random ports from the configured ephemeral range.
	// If proximity fails, we assume the NAT might allocate from the standard ephemeral pool.
	for len(ports) < count {
		rangeSize := s.portMax - s.portMin + 1
		p := s.portMin + s.rand.Intn(rangeSize)
		// Use configured limits (e.g. 49152-65535) for random guessing
		if s.addPort(p, &ports, count, s.portMin, s.portMax) {
			break
		}
	}

Done:
	s.attempts++

	addrs := make([]string, 0, len(ports))
	for _, p := range ports {
		addrs = append(addrs, net.JoinHostPort(s.host, strconv.Itoa(p)))
	}
	return addrs
}

func (s *birthdayPunchState) addPort(p int, ports *[]int, max int, limitMin, limitMax int) bool {
	if len(*ports) >= max {
		return true
	}
	// Respect the specific limits passed for this strategy (Proximity vs Random)
	if p < limitMin || p > limitMax {
		return false
	}
	if _, ok := s.seen[p]; ok {
		return false
	}
	s.seen[p] = struct{}{}
	*ports = append(*ports, p)
	return len(*ports) >= max
}

func pickBirthdayHost(addrs []string) (string, []int) {
	var host string
	for _, addr := range addrs {
		h, _, ok := splitHostPort(addr)
		if !ok {
			continue
		}
		if isPublicIPv4(h) {
			host = h
			break
		}
	}
	if host == "" {
		return "", nil
	}

	ports := make([]int, 0, len(addrs))
	for _, addr := range addrs {
		h, p, ok := splitHostPort(addr)
		if !ok || h != host {
			continue
		}
		ports = append(ports, p)
	}
	return host, ports
}

func splitHostPort(addr string) (string, int, bool) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, false
	}
	return host, port, true
}

func isPublicIPv4(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return false
	}
	if ip.IsPrivate() {
		return false
	}
	// Tailscale CGNAT range
	if ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
		return false
	}
	return true
}

func newBirthdayRand(cfg config) *rand.Rand {
	if cfg.birthdayPunchSeeded {
		return rand.New(rand.NewSource(int64(cfg.birthdayPunchSeed)))
	}

	var seed uint64
	var b [8]byte
	if _, err := crand.Read(b[:]); err == nil {
		seed = binary.LittleEndian.Uint64(b[:])
	} else {
		seed = uint64(time.Now().UnixNano())
	}

	return rand.New(rand.NewSource(int64(seed)))
}
