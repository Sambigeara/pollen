package node

import (
	"encoding/base64"
	"net"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
)

// NewInvite creates a new invite token containing the bootstrap node's ed25519
// public key (fingerprint). The joiner uses this to verify the bootstrap peer.
func NewInvite(ips []string, port string, fingerprint []byte) (*admissionv1.Invite, error) {
	id := uuid.NewString()

	addrs := make([]string, len(ips))
	for i, ip := range ips {
		addrs[i] = net.JoinHostPort(ip, port)
	}

	return &admissionv1.Invite{
		Id:          id,
		Addr:        addrs,
		Fingerprint: fingerprint,
	}, nil
}

func EncodeToken(token *admissionv1.Invite) (string, error) {
	b, err := proto.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func DecodeToken(s string) (*admissionv1.Invite, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}

	token := &admissionv1.Invite{}
	if err := proto.Unmarshal(b, token); err != nil {
		return nil, err
	}

	return token, nil
}
