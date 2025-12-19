package link

import (
	"crypto/rand"
	"testing"

	"github.com/flynn/noise"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ed25519"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/types"
)

func TestHandshake_Pure_XX(t *testing.T) {
	// Node A (Responder)
	csA := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)
	kpA, _ := csA.GenerateKeypair(rand.Reader)
	pubA, _, _ := ed25519.GenerateKey(rand.Reader)
	invitesA, _ := admission.Load(t.TempDir())

	// Node B (Initiator)
	csB := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)
	kpB, _ := csB.GenerateKeypair(rand.Reader)
	pubB, _, _ := ed25519.GenerateKey(rand.Reader)

	// Create Invite
	psk := make([]byte, 32)
	rand.Read(psk)
	invite := &peerv1.Invite{
		Id:   uuid.NewString(),
		Psk:  psk,
		Addr: []string{"1.2.3.4:5000"},
	}
	invitesA.AddInvite(invite)

	// Setup Handshakes
	hsInit, err := newHandshakeXXPsk2Init(&csB, kpB, pubB, invite)
	require.NoError(t, err)

	hsResp, err := newHandshakeXXPsk2Resp(&csA, invitesA, kpA, pubA)
	require.NoError(t, err)

	// 1. Init (B) -> Msg1
	res1, err := hsInit.Step(nil)
	require.NoError(t, err)
	assert.Equal(t, types.MsgTypeHandshakeXXPsk2Init, res1.MsgType)
	assert.NotEmpty(t, res1.Msg)

	// 2. Resp (A) processes Msg1 -> Msg2
	res2, err := hsResp.Step(res1.Msg)
	require.NoError(t, err)
	assert.Equal(t, types.MsgTypeHandshakeXXPsk2Resp, res2.MsgType)
	assert.NotEmpty(t, res2.Msg)

	// 3. Init (B) processes Msg2 -> Msg3 + Session
	res3, err := hsInit.Step(res2.Msg)
	require.NoError(t, err)
	assert.Equal(t, types.MsgTypeHandshakeXXPsk2Init, res3.MsgType)
	assert.NotEmpty(t, res3.Msg)
	require.NotNil(t, res3.Session)
	assert.Equal(t, kpA.Public, res3.PeerStaticKey)

	// 4. Resp (A) processes Msg3 -> Session
	res4, err := hsResp.Step(res3.Msg)
	require.NoError(t, err)
	require.NotNil(t, res4.Session)
	assert.Equal(t, kpB.Public, res4.PeerStaticKey)

	assert.EqualValues(t, pubA, res3.PeerIdentityPub)
	assert.EqualValues(t, pubB, res4.PeerIdentityPub)

	// 5. Verify Encryption Round-Trip
	verifySession(t, res3.Session, res4.Session)
}

func TestHandshake_Pure_IK(t *testing.T) {
	// Node A (Responder)
	csA := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)
	kpA, _ := csA.GenerateKeypair(rand.Reader)

	// Node B (Initiator)
	csB := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)
	kpB, _ := csB.GenerateKeypair(rand.Reader)

	// IK requires Initiator (B) to already know Responder's (A) static key
	hsInit, err := newHandshakeIKInit(&csB, kpB, kpA.Public)
	require.NoError(t, err)

	hsResp, err := newHandshakeIKResp(&csA, kpA)
	require.NoError(t, err)

	// 1. Init (B) -> Msg1
	res1, err := hsInit.Step(nil)
	require.NoError(t, err)
	assert.Equal(t, types.MsgTypeHandshakeIKInit, res1.MsgType)
	assert.NotEmpty(t, res1.Msg)

	// 2. Resp (A) processes Msg1 -> Msg2 + Session
	res2, err := hsResp.Step(res1.Msg)
	require.NoError(t, err)
	assert.Equal(t, types.MsgTypeHandshakeIKResp, res2.MsgType)
	assert.NotEmpty(t, res2.Msg)
	require.NotNil(t, res2.Session) // IK establishes responder session immediately
	assert.Equal(t, kpB.Public, res2.PeerStaticKey)

	// 3. Init (B) processes Msg2 -> Session
	res3, err := hsInit.Step(res2.Msg)
	require.NoError(t, err)
	require.NotNil(t, res3.Session)
	assert.Equal(t, kpA.Public, res3.PeerStaticKey)

	// 4. Verify Encryption Round-Trip
	verifySession(t, res3.Session, res2.Session)
}

func verifySession(t *testing.T, a, b *session) {
	t.Helper()

	msg := []byte("hello world")

	// A Encrypts
	enc, _, err := a.Encrypt(msg)
	require.NoError(t, err)

	// B Decrypts
	dec, _, err := b.Decrypt(enc)
	require.NoError(t, err)
	assert.Equal(t, msg, dec)

	// B Encrypts
	enc2, _, err := b.Encrypt(msg)
	require.NoError(t, err)

	// A Decrypts
	dec2, _, err := a.Decrypt(enc2)
	require.NoError(t, err)
	assert.Equal(t, msg, dec2)
}
