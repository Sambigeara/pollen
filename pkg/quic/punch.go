package quic

import (
	"math/rand"
	"net"
	"time"
)

const (
	punchRemoteRandomPortAttempts = 256
	punchLocalSocketAttempts      = 256
	punchPortMin                  = 1024
	punchPortMax                  = 65535
)

func buildPunchCandidates(target *net.UDPAddr, randomCount int) []*net.UDPAddr {
	if target == nil || target.IP == nil {
		return nil
	}
	if target.Port < 1 || target.Port > punchPortMax {
		return nil
	}
	if randomCount < 0 {
		randomCount = 0
	}
	maxUnique := punchPortMax - punchPortMin + 1
	if randomCount > maxUnique {
		randomCount = maxUnique
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	rangeSize := punchPortMax - punchPortMin + 1
	candidates := make([]*net.UDPAddr, 0, randomCount+1)
	seen := make(map[int]struct{}, randomCount+1)

	seen[target.Port] = struct{}{}
	candidates = append(candidates, clonePunchAddr(target, target.Port))

	for len(candidates)-1 < randomCount {
		port := punchPortMin + rnd.Intn(rangeSize)
		if _, ok := seen[port]; ok {
			continue
		}
		seen[port] = struct{}{}
		candidates = append(candidates, clonePunchAddr(target, port))
	}

	return candidates
}

func clonePunchAddr(base *net.UDPAddr, port int) *net.UDPAddr {
	ip := append(net.IP(nil), base.IP...)
	return &net.UDPAddr{IP: ip, Port: port, Zone: base.Zone}
}
