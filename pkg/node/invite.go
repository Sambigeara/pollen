package node

import (
	"crypto/rand"
	"encoding/base64"
	"net"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
)

func NewInvite(ips []net.IP, port string) (*peerv1.Invite, error) {
	id := uuid.NewString()

	// TODO(saml) PSK should have expiry
	psk, err := generatePSK()
	if err != nil {
		return nil, err
	}

	addrs := make([]string, len(ips))
	for i, ip := range ips {
		addrs[i] = net.JoinHostPort(ip.String(), port)
	}

	return &peerv1.Invite{
		Id:   id,
		Psk:  psk,
		Addr: addrs,
	}, nil
}

func generatePSK() ([]byte, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func EncodeToken(token *peerv1.Invite) (string, error) {
	b, err := proto.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func DecodeToken(s string) (*peerv1.Invite, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}

	token := &peerv1.Invite{}
	if err := proto.Unmarshal(b, token); err != nil {
		return nil, err
	}

	return token, nil
}
