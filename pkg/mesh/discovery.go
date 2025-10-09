package mesh

import (
	"errors"
	"io"
	"net"
	"net/http"
)

func DiscoverIP() (net.IP, error) {
	resp, err := http.Get("https://api.ipify.org")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	ip := net.ParseIP(string(b))
	if ip == nil {
		return nil, errors.New("failed to parse retrieved IP")
	}

	return ip, nil
}
