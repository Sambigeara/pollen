package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const publicIPQueryTimeout = time.Second * 5

var providers = []string{
	"https://api64.ipify.org",
	"https://ifconfig.me/ip",
	"https://icanhazip.com",
	"https://ident.me",
}

func GetAdvertisableAddrs() ([]string, error) {
	var results []string
	seen := make(map[string]bool)

	ctx, cancelFn := context.WithTimeout(context.Background(), publicIPQueryTimeout)
	defer cancelFn()

	if pubIP, err := getPublicIP(ctx); err == nil {
		str := pubIP.String()
		results = append(results, pubIP.String())
		seen[str] = true
	}

	if prefIP, err := getPreferredOutboundIP(ctx); err == nil {
		str := prefIP.String()
		if !seen[str] {
			results = append(results, prefIP.String())
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
func getPublicIP(ctx context.Context) (net.IP, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ipCh := make(chan net.IP, 1)
	var wg sync.WaitGroup

	for _, url := range providers {
		wg.Add(1)
		go func(providerURL string) {
			defer wg.Done()

			req, err := http.NewRequestWithContext(ctx, "GET", providerURL, nil)
			if err != nil {
				return
			}

			client := &http.Client{Timeout: 3 * time.Second}
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
		}(url)
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
	// conn, err := net.Dial("udp", "8.8.8.8:80")
	d := &net.Dialer{
		Timeout: 5 * time.Second,
	}
	conn, err := d.DialContext(ctx, "udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
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
		if i.Flags&net.FlagUp == 0 || i.Flags&net.FlagLoopback != 0 {
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
	return ip != nil && !ip.IsLoopback() && !ip.IsMulticast() && !ip.IsUnspecified() && !ip.IsLinkLocalUnicast()
}
