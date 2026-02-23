package mesh

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
)

const publicIPQueryTimeout = 3 * time.Second

var (
	TailscaleCGNAT = netip.MustParsePrefix("100.64.0.0/10") // RFC 6598 CGNAT
	TailscaleULA   = netip.MustParsePrefix("fd7a:115c:a1e0::/48")

	Private10  = netip.MustParsePrefix("10.0.0.0/8")     // RFC 1918
	Private172 = netip.MustParsePrefix("172.16.0.0/12")  // RFC 1918
	Private192 = netip.MustParsePrefix("192.168.0.0/16") // RFC 1918

	DefaultExclusions = []netip.Prefix{TailscaleCGNAT, TailscaleULA}
)

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

func GetAdvertisableAddrs(exclusions []netip.Prefix) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), publicIPQueryTimeout)
	defer cancel()

	publicIPs := getPublicIPs(ctx)
	localIPs := getLocalIPs(ctx)
	all := make([]netip.Addr, 0, len(publicIPs)+len(localIPs))
	all = append(all, publicIPs...)
	all = append(all, localIPs...)

	seen := make(map[netip.Addr]bool, len(all))
	results := make([]string, 0, len(all))
	for _, ip := range all {
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
		return nil, errors.New("could not determine any advertisable IP addresses")
	}
	return results, nil
}

func getPublicIPs(ctx context.Context) []netip.Addr {
	var out []netip.Addr
	if ip, err := raceProviders(ctx, ipv4Providers); err == nil {
		out = append(out, ip)
	}
	if ip, err := raceProviders(ctx, ipv6Providers); err == nil {
		out = append(out, ip)
	}
	return out
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
	for _, r := range ranges {
		if r.Contains(ip) {
			return true
		}
	}
	return false
}

func raceProviders(ctx context.Context, providers []string) (netip.Addr, error) {
	ctx, cancel := context.WithTimeout(ctx, publicIPQueryTimeout)
	defer cancel()

	ch := make(chan netip.Addr, 1)
	var wg sync.WaitGroup

	for _, url := range providers {
		wg.Go(func() {
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
		})
	}

	select {
	case ip := <-ch:
		return ip, nil
	case <-ctx.Done():
		return netip.Addr{}, errors.New("timed out resolving public IP")
	}
}

// Dials a UDP socket to a public address without sending any data.
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
