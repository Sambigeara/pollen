package link

import (
	"crypto/rand"
	"testing"

	"github.com/flynn/noise"
	"github.com/stretchr/testify/require"
)

func newSessionPair(t *testing.T) (*session, *session) {
	t.Helper()

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)
	kpA, _ := cs.GenerateKeypair(rand.Reader)
	kpB, _ := cs.GenerateKeypair(rand.Reader)

	hsInit, err := newHandshakeIKInit(&cs, kpB, kpA.Public)
	require.NoError(t, err)

	hsResp, err := newHandshakeIKResp(&cs, kpA)
	require.NoError(t, err)

	res1, err := hsInit.Step(nil)
	require.NoError(t, err)

	res2, err := hsResp.Step(res1.Msg)
	require.NoError(t, err)

	res3, err := hsInit.Step(res2.Msg)
	require.NoError(t, err)

	require.NotNil(t, res2.Session)
	require.NotNil(t, res3.Session)

	return res3.Session, res2.Session
}

func TestSessionDecrypt_OutOfOrder(t *testing.T) {
	init, resp := newSessionPair(t)

	msg1 := []byte("one")
	msg2 := []byte("two")

	ct1, _, err := init.Encrypt(msg1)
	require.NoError(t, err)
	ct2, _, err := init.Encrypt(msg2)
	require.NoError(t, err)

	pt2, _, err := resp.Decrypt(ct2)
	require.NoError(t, err)
	require.Equal(t, msg2, pt2)

	pt1, _, err := resp.Decrypt(ct1)
	require.NoError(t, err)
	require.Equal(t, msg1, pt1)
}

func TestSessionDecrypt_Replay(t *testing.T) {
	init, resp := newSessionPair(t)

	ct, _, err := init.Encrypt([]byte("ping"))
	require.NoError(t, err)

	pt, _, err := resp.Decrypt(ct)
	require.NoError(t, err)
	require.Equal(t, "ping", string(pt))

	_, _, err = resp.Decrypt(ct)
	require.ErrorIs(t, err, ErrReplay)
}

func TestSessionDecrypt_TooOld(t *testing.T) {
	init, resp := newSessionPair(t)

	messages := make([][]byte, recvWindowSize+1)
	for i := range messages {
		ct, _, err := init.Encrypt([]byte{byte(i)})
		require.NoError(t, err)
		messages[i] = ct
	}

	for _, ct := range messages {
		_, _, err := resp.Decrypt(ct)
		require.NoError(t, err)
	}

	_, _, err := resp.Decrypt(messages[0])
	require.ErrorIs(t, err, ErrTooOld)
}
