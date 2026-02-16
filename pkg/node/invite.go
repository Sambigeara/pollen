package node

import (
	"encoding/base64"
	"net"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
)

// NewInvite creates a new invite token containing the bootstrap node's ed25519
// public key (fingerprint). The joiner uses this to verify the bootstrap peer.
func NewInvite(ips []string, port string, fingerprint []byte) (*peerv1.Invite, error) {
	id := uuid.NewString()

	addrs := make([]string, len(ips))
	for i, ip := range ips {
		addrs[i] = net.JoinHostPort(ip, port)
	}

	return &peerv1.Invite{
		Id:          id,
		Addr:        addrs,
		Fingerprint: fingerprint,
	}, nil
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
