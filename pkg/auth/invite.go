package auth

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
)

const inviteExpirySkew = time.Minute

// InviteConsumer tracks which invite tokens have been redeemed.
type InviteConsumer interface {
	TryConsume(token *admissionv1.InviteToken, now time.Time) (bool, error)
	Export() []*statev1.ConsumedInvite
}

type impl struct {
	consumed map[string]int64
	mu       sync.Mutex
}

func NewInviteConsumer(entries []*statev1.ConsumedInvite) InviteConsumer {
	consumed := make(map[string]int64, len(entries))
	for _, e := range entries {
		if e.GetTokenId() != "" {
			consumed[e.GetTokenId()] = e.GetExpiryUnix()
		}
	}
	return &impl{consumed: consumed}
}

func (c *impl) TryConsume(token *admissionv1.InviteToken, now time.Time) (bool, error) {
	claims := token.GetClaims()
	if claims == nil {
		return false, errors.New("invite token missing claims")
	}
	tokenID := claims.GetTokenId()
	if tokenID == "" {
		return false, errors.New("invite token missing token id")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.consumed[tokenID]; exists {
		return false, nil
	}

	cutoff := now.Unix() - int64(inviteExpirySkew/time.Second)
	for id, exp := range c.consumed {
		if exp > 0 && exp < cutoff {
			delete(c.consumed, id)
		}
	}

	c.consumed[tokenID] = claims.GetExpiresAtUnix()
	return true, nil
}

// Export returns the consumed invite set as a proto slice for persistence.
func (c *impl) Export() []*statev1.ConsumedInvite {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*statev1.ConsumedInvite, 0, len(c.consumed))
	for id, expiry := range c.consumed {
		out = append(out, &statev1.ConsumedInvite{
			TokenId:    id,
			ExpiryUnix: expiry,
		})
	}
	return out
}

func VerifyInviteToken(token *admissionv1.InviteToken, expectedSubject ed25519.PublicKey, now time.Time) error {
	if err := protovalidate.Validate(token); err != nil {
		return fmt.Errorf("invite token invalid: %w", err)
	}

	claims := token.GetClaims()
	issuer := claims.GetIssuer()
	if IsCertExpired(issuer, now) {
		return fmt.Errorf("invite token issuer: %w", ErrCertExpired)
	}

	issuerPub := issuer.GetClaims().GetSubjectPub()
	msg, err := signaturePayload(claims)
	if err != nil {
		return err
	}
	if err := verifyPayload(ed25519.PublicKey(issuerPub), msg, token.GetSignature(), sigContextInvite); err != nil {
		return errors.New("invite token signature invalid")
	}

	if len(expectedSubject) > 0 && len(claims.GetSubjectPub()) > 0 && !bytes.Equal(claims.GetSubjectPub(), expectedSubject) {
		return errors.New("invite token subject mismatch")
	}

	issuedAt := time.Unix(claims.GetIssuedAtUnix(), 0).Add(-timeSkewAllowance)
	expiresAt := time.Unix(claims.GetExpiresAtUnix(), 0).Add(timeSkewAllowance)
	if !expiresAt.After(issuedAt) {
		return errors.New("invite token validity window invalid")
	}
	if now.Before(issuedAt) || now.After(expiresAt) {
		return errors.New("invite token expired or not yet valid")
	}

	return nil
}

func EncodeInviteToken(token *admissionv1.InviteToken) (string, error) {
	b, err := token.MarshalVT()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func DecodeInviteToken(s string) (*admissionv1.InviteToken, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}

	token := &admissionv1.InviteToken{}
	if err := token.UnmarshalVT(b); err != nil {
		return nil, err
	}

	return token, nil
}
