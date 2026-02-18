package mesh

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const publicIPQueryTimeout = time.Second * 3

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

func GetAdvertisableAddrs() ([]string, error) {
	var results []string
	seen := make(map[string]bool)

	ctx, cancelFn := context.WithTimeout(context.Background(), publicIPQueryTimeout)
	defer cancelFn()

	if pubIP, err := getPublicIP(ctx, ipv4Providers); err == nil {
		str := pubIP.String()
		results = append(results, str)
		seen[str] = true
	}

	// Try resolving IPv6 independently. Even if IPv4 already worked we still want IPv6 addresses
	// to be advertised for dual-stack peers, but we keep IPv4 earlier in the list to favour it.
	if pubIPv6, err := getPublicIP(ctx, ipv6Providers); err == nil {
		str := pubIPv6.String()
		if !seen[str] {
			results = append(results, str)
			seen[str] = true
		}
	}

	if prefIP, err := getPreferredOutboundIP(ctx); err == nil && isValidIP(prefIP) {
		str := prefIP.String()
		if !seen[str] {
			results = append(results, str)
			seen[str] = true
		}
	}

	others, err := getOtherLocalIPs()
	if err == nil {
		for _, ip := range others {
			if !seen[ip.String()] {
				results = append(results, ip.String())
				seen[ip.String()] = true
			}
		}
	}

	if len(results) == 0 {
		return nil, errors.New("could not determine any advertisable IP addresses")
	}

	return results, nil
}

// getPublicIP races multiple providers to return the first valid response.
func getPublicIP(ctx context.Context, providers []string) (net.IP, error) {
	ctx, cancel := context.WithTimeout(ctx, publicIPQueryTimeout)
	defer cancel()

	ipCh := make(chan net.IP, 1)
	var wg sync.WaitGroup

	for _, url := range providers {
		wg.Go(func() {
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				return
			}

			client := &http.Client{Timeout: publicIPQueryTimeout}
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return
			}

			ipStr := strings.TrimSpace(string(body))
			ip := net.ParseIP(ipStr)

			if isValidIP(ip) {
				select {
				case ipCh <- ip:
				case <-ctx.Done():
				}
			}
		})
	}

	select {
	case ip := <-ipCh:
		return ip, nil
	case <-ctx.Done():
		return nil, errors.New("timed out resolving public IP")
	}
}

// getPreferredOutboundIP determines the local IP used to route to the internet.
// This does not actually establish a connection.
func getPreferredOutboundIP(ctx context.Context) (net.IP, error) {
	// 8.8.8.8 is used as a reference to determine the default route.
	d := &net.Dialer{
		Timeout: publicIPQueryTimeout,
	}
	conn, err := d.DialContext(ctx, "udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr) //nolint:forcetypeassert
	return localAddr.IP, nil
}

// getOtherLocalIPs iterates all network interfaces for valid addresses.
func getOtherLocalIPs() ([]net.IP, error) {
	var ips []net.IP
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, i := range interfaces {
		if i.Flags&net.FlagUp == 0 || i.Flags&net.FlagLoopback != 0 || isDockerInterface(i.Name) {
			continue
		}

		addrs, err := i.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if isValidIP(ip) {
				ips = append(ips, ip)
			}
		}
	}
	return ips, nil
}

// isValidIP filters out Loopback, Multicast, Unspecified, and Link-Local (fe80::) addresses.
func isValidIP(ip net.IP) bool {
	if ip == nil {
		return false
	}

	if ip.IsLoopback() || ip.IsMulticast() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() {
		return false
	}

	if ip4 := ip.To4(); ip4 != nil {
		// Ignore Tailscale IPs (CGNAT range 100.64.0.0/10).
		if ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
			return false
		}
	}

	ip16 := ip.To16()
	if ip16 == nil {
		return false
	}
	// Ignore Tailscale ULA fd7a:115c:a1e0::/48.
	if ip16[0] == 0xfd && ip16[1] == 0x7a && ip16[2] == 0x11 && ip16[3] == 0x5c && ip16[4] == 0xa1 && ip16[5] == 0xe0 {
		return false
	}

	//nolint:mnd
	if _, ignoreLocal := os.LookupEnv("IP_IGNORE_LOCAL"); ignoreLocal { //nolint:nestif
		if ip4 := ip.To4(); ip4 != nil {
			// 10.0.0.0/8
			if ip4[0] == 10 {
				return false
			}
			// 172.16.0.0/12
			if ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31 {
				return false
			}
			// 192.168.0.0/16
			if ip4[0] == 192 && ip4[1] == 168 {
				return false
			}
		}
	}
	return true
}

// isDockerInterface returns true for interfaces commonly created by Docker
// (docker0, br-, veth).
func isDockerInterface(name string) bool {
	return name == "docker0" ||
		strings.HasPrefix(name, "br-") ||
		strings.HasPrefix(name, "veth")
}
