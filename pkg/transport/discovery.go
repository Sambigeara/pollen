package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"time"
)

const publicIPQueryTimeout = 3 * time.Second

var DefaultExclusions = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),       // RFC 6598 CGNAT (Tailscale)
	netip.MustParsePrefix("fd7a:115c:a1e0::/48"), // Tailscale ULA
}

var (
	ipv4Providers = []string{
		"https://api.ipify.org",
		"https://ipv4.icanhazip.com",
		"https://ifconfig.me/ip",
	}
	ipv6Providers = []string{
		"https://api64.ipify.org",
		"https://icanhazip.com",
		"https://ident.me",
	}
)

// GetLocalInterfaceAddrs returns routable IPs from local network interfaces,
// excluding loopback, link-local, virtual, and any prefixes in exclusions.
func GetLocalInterfaceAddrs(exclusions []netip.Prefix) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), publicIPQueryTimeout)
	defer cancel()

	localIPs := getLocalIPs(ctx)

	seen := make(map[netip.Addr]bool, len(localIPs))
	results := make([]string, 0, len(localIPs))
	for _, ip := range localIPs {
		if seen[ip] {
			continue
		}
		if !isRoutable(ip) || isExcluded(ip, exclusions) {
			continue
		}
		seen[ip] = true
		results = append(results, ip.String())
	}

	if len(results) == 0 {
		return nil, errors.New("could not determine any local interface addresses")
	}
	return results, nil
}

// GetPublicIP queries external providers for the node's public IP.
// Returns a single IP string (preferring IPv4), or empty string on failure.
func GetPublicIP() string {
	ctx, cancel := context.WithTimeout(context.Background(), publicIPQueryTimeout)
	defer cancel()

	if ip, err := raceProviders(ctx, ipv4Providers); err == nil {
		return ip.String()
	}
	if ip, err := raceProviders(ctx, ipv6Providers); err == nil {
		return ip.String()
	}
	return ""
}

func getLocalIPs(ctx context.Context) []netip.Addr {
	var out []netip.Addr

	if ip, err := preferredOutboundIP(ctx); err == nil {
		out = append(out, ip)
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		return out
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || isVirtualInterface(iface.Name) {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if ipNet, ok := a.(*net.IPNet); ok {
				if addr, ok := netip.AddrFromSlice(ipNet.IP); ok {
					out = append(out, addr.Unmap())
				}
			}
		}
	}
	return out
}

func isRoutable(ip netip.Addr) bool {
	return ip.IsValid() &&
		!ip.IsLoopback() &&
		!ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() &&
		!ip.IsMulticast() &&
		!ip.IsUnspecified()
}

func isExcluded(ip netip.Addr, ranges []netip.Prefix) bool {
	return slices.ContainsFunc(ranges, func(r netip.Prefix) bool {
		return r.Contains(ip)
	})
}

func raceProviders(ctx context.Context, providers []string) (netip.Addr, error) {
	ctx, cancel := context.WithTimeout(ctx, publicIPQueryTimeout)
	defer cancel()

	ch := make(chan netip.Addr, len(providers))

	for _, url := range providers {
		go func() {
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				return
			}
			resp, err := (&http.Client{Timeout: publicIPQueryTimeout}).Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return
			}

			addr, err := netip.ParseAddr(strings.TrimSpace(string(body)))
			if err != nil || !addr.IsValid() {
				return
			}
			select {
			case ch <- addr.Unmap():
			case <-ctx.Done():
			}
		}()
	}

	select {
	case ip := <-ch:
		return ip, nil
	case <-ctx.Done():
		return netip.Addr{}, errors.New("timed out resolving public IP")
	}
}

func preferredOutboundIP(ctx context.Context) (netip.Addr, error) {
	conn, err := (&net.Dialer{Timeout: publicIPQueryTimeout}).DialContext(ctx, "udp", "8.8.8.8:80")
	if err != nil {
		return netip.Addr{}, err
	}
	defer conn.Close()

	udpAddr := conn.LocalAddr().(*net.UDPAddr) //nolint:forcetypeassert
	addr, ok := netip.AddrFromSlice(udpAddr.IP)
	if !ok {
		return netip.Addr{}, errors.New("invalid local address")
	}
	return addr.Unmap(), nil
}

func isVirtualInterface(name string) bool {
	return name == "docker0" ||
		strings.HasPrefix(name, "br-") ||
		strings.HasPrefix(name, "veth")
}
