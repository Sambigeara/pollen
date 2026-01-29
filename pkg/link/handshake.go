package link

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	preSharedKeyPlacement = 2
	sessionIDLength       = 4
)

var (
	_ handshake = (*handshakeIKInit)(nil)
	_ handshake = (*handshakeIKResp)(nil)
	_ handshake = (*handshakeXXPsk2Init)(nil)
	_ handshake = (*handshakeXXPsk2Resp)(nil)
)

var (
	ErrNoResolvableIP = errors.New("no resolvable IP from peer")
	ErrEmptyMsg       = errors.New("no message to send")

	handshakePrologue = []byte("pollenv1")
)

type HandshakeResult struct {
	Session         *session
	Msg             []byte
	PeerStaticKey   []byte
	PeerIdentityPub []byte
	MsgType         types.MsgType
	LocalSessionID  uint32
}

type handshake interface {
	Step(msg []byte) (HandshakeResult, error)
}

type handshakeStore struct {
	invites          admission.Admission
	cs               *noise.CipherSuite
	st               map[uint32]handshake
	localStaticKey   noise.DHKey
	localIdentityPub []byte
	mu               sync.RWMutex
}

func newHandshakeStore(cs *noise.CipherSuite, invites admission.Admission, localStaticKey noise.DHKey, localSigPub []byte) *handshakeStore {
	return &handshakeStore{
		cs:               cs,
		invites:          invites,
		localStaticKey:   localStaticKey,
		localIdentityPub: localSigPub,
		st:               make(map[uint32]handshake),
		mu:               sync.RWMutex{},
	}
}

func (st *handshakeStore) getOrCreate(peerSessionID, localSessionID uint32, tp types.MsgType) (handshake, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if hs, ok := st.st[peerSessionID]; ok {
		return hs, nil
	}

	var hs handshake
	var err error

	switch tp { //nolint:exhaustive
	case types.MsgTypeHandshakeIKResp, types.MsgTypeHandshakeXXPsk2Resp:
		// If we init a handshake, we set the handshake to the localSessionID.
		// These events will be the response to our init, so we swop out the local key for the peer one.
		var ok bool
		hs, ok = st.st[localSessionID]
		if !ok {
			return nil, nil
		}
		delete(st.st, localSessionID)
	case types.MsgTypeHandshakeIKInit:
		hs, err = newHandshakeIKResp(st.cs, st.localStaticKey)
		if err != nil {
			return nil, err
		}
	case types.MsgTypeHandshakeXXPsk2Init:
		hs, err = newHandshakeXXPsk2Resp(st.cs, st.invites, st.localStaticKey, st.localIdentityPub)
		if err != nil {
			return nil, err
		}
	}

	st.st[peerSessionID] = hs
	return hs, err
}

func (st *handshakeStore) initIK(peerStaticKey []byte) (HandshakeResult, error) {
	hs, err := newHandshakeIKInit(st.cs, st.localStaticKey, peerStaticKey)
	if err != nil {
		return HandshakeResult{}, err
	}

	st.mu.Lock()
	st.st[hs.localSessionID] = hs
	st.mu.Unlock()

	// Advance to stage 1 to generate the Init packet
	return hs.Step(nil)
}

func (st *handshakeStore) initXXPsk2(token *peerv1.Invite, peerSigPub []byte) (HandshakeResult, error) {
	hs, err := newHandshakeXXPsk2Init(st.cs, st.localStaticKey, peerSigPub, token)
	if err != nil {
		return HandshakeResult{}, err
	}

	st.mu.Lock()
	st.st[hs.localSessionID] = hs
	st.mu.Unlock()

	// Advance to stage 1 to generate the Init packet
	return hs.Step(nil)
}

func (st *handshakeStore) clear(peerSessionID uint32, after time.Duration) {
	time.AfterFunc(after, func() {
		st.mu.Lock()
		defer st.mu.Unlock()
		delete(st.st, peerSessionID)
	})
}

type handshakeStage int

const (
	stage1 handshakeStage = iota
	stage2
)

type handshakeIKInit struct {
	*noise.HandshakeState
	localSessionID uint32
	nextStage      handshakeStage
	mu             sync.Mutex
}

func newHandshakeIKInit(cs *noise.CipherSuite, localStaticKey noise.DHKey, peerStaticKey []byte) (*handshakeIKInit, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   *cs,
		Pattern:       noise.HandshakeIK,
		Initiator:     true,
		Prologue:      handshakePrologue,
		StaticKeypair: localStaticKey,
		PeerStatic:    peerStaticKey,
	})
	if err != nil {
		return nil, err
	}

	localSessionID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	return &handshakeIKInit{
		HandshakeState: hs,
		localSessionID: localSessionID,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeIKInit) Step(rcvMsg []byte) (HandshakeResult, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	res := HandshakeResult{
		MsgType:        types.MsgTypeHandshakeIKInit,
		LocalSessionID: hs.localSessionID,
	}

	switch hs.nextStage {
	case stage1:
		msg1, _, _, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return res, err
		}
		res.Msg = msg1

	case stage2:
		_, csSend, csRecv, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return res, err
		}

		res.Session = newSession(hs.localSessionID, hs.PeerStatic(), csSend, csRecv, true) // we initiated
		res.PeerStaticKey = hs.PeerStatic()

	default:
		return res, fmt.Errorf("unexpected stage: %v", hs.nextStage)
	}

	hs.nextStage++
	return res, nil
}

type handshakeIKResp struct {
	*noise.HandshakeState
	localSessionID uint32
	nextStage      handshakeStage
	mu             sync.Mutex
}

func newHandshakeIKResp(cs *noise.CipherSuite, localStaticKey noise.DHKey) (*handshakeIKResp, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   *cs,
		Pattern:       noise.HandshakeIK,
		Prologue:      handshakePrologue,
		StaticKeypair: localStaticKey,
	})
	if err != nil {
		return nil, err
	}

	localSessionID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	return &handshakeIKResp{
		HandshakeState: hs,
		localSessionID: localSessionID,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeIKResp) Step(rcvMsg []byte) (HandshakeResult, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	res := HandshakeResult{
		MsgType:        types.MsgTypeHandshakeIKResp,
		LocalSessionID: hs.localSessionID,
	}

	switch hs.nextStage {
	case stage1:
		if _, _, _, err := hs.ReadMessage(nil, rcvMsg); err != nil {
			return res, err
		}

		msg2, csRecv, csSend, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return res, err
		}

		res.Msg = msg2
		res.Session = newSession(hs.localSessionID, hs.PeerStatic(), csSend, csRecv, false) // we responded
		res.PeerStaticKey = hs.PeerStatic()

	default:
		return res, fmt.Errorf("unexpected stage: %v", hs.nextStage)
	}

	hs.nextStage++
	return res, nil
}

type handshakeXXPsk2Init struct {
	*noise.HandshakeState
	tokenID        string
	sigPub         []byte
	localSessionID uint32
	nextStage      handshakeStage
	mu             sync.Mutex
}

func newHandshakeXXPsk2Init(cs *noise.CipherSuite, localStaticKey noise.DHKey, peerSigPub []byte, token *peerv1.Invite) (*handshakeXXPsk2Init, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:           *cs,
		Pattern:               noise.HandshakeXX,
		Initiator:             true,
		Prologue:              handshakePrologue,
		PresharedKeyPlacement: preSharedKeyPlacement,
		PresharedKey:          token.Psk,
		StaticKeypair:         localStaticKey,
	})
	if err != nil {
		return nil, err
	}

	localSessionID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	return &handshakeXXPsk2Init{
		HandshakeState: hs,
		sigPub:         peerSigPub,
		localSessionID: localSessionID,
		tokenID:        token.Id,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeXXPsk2Init) Step(rcvMsg []byte) (HandshakeResult, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	res := HandshakeResult{
		MsgType:        types.MsgTypeHandshakeXXPsk2Init,
		LocalSessionID: hs.localSessionID,
	}

	switch hs.nextStage {
	case stage1:
		msg1, _, _, err := hs.WriteMessage(nil, []byte(hs.tokenID))
		if err != nil {
			return res, err
		}
		// Happy Eyeballs: Return all candidates as targets
		res.Msg = msg1

	case stage2:
		peerIdentityPub, _, _, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return res, err
		}

		msg3, csSend, csRecv, err := hs.WriteMessage(nil, hs.sigPub)
		if err != nil {
			return res, err
		}

		res.Msg = msg3
		res.Session = newSession(hs.localSessionID, hs.PeerStatic(), csSend, csRecv, true) // we initiated
		res.PeerStaticKey = hs.PeerStatic()
		res.PeerIdentityPub = peerIdentityPub

	default:
		return res, fmt.Errorf("unexpected stage: %v", hs.nextStage)
	}

	hs.nextStage++
	return res, nil
}

type handshakeXXPsk2Resp struct {
	*noise.HandshakeState
	admission      admission.Admission
	sigPub         []byte
	localSessionID uint32
	nextStage      handshakeStage
	mu             sync.Mutex
}

func newHandshakeXXPsk2Resp(cs *noise.CipherSuite, invitesStore admission.Admission, localStaticKey noise.DHKey, localSigPub []byte) (*handshakeXXPsk2Resp, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:           *cs,
		Pattern:               noise.HandshakeXX,
		Prologue:              handshakePrologue,
		PresharedKeyPlacement: preSharedKeyPlacement,
		StaticKeypair:         localStaticKey,
	})
	if err != nil {
		return nil, err
	}

	localSessionID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	return &handshakeXXPsk2Resp{
		HandshakeState: hs,
		admission:      invitesStore,
		sigPub:         localSigPub,
		localSessionID: localSessionID,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeXXPsk2Resp) Step(rcvMsg []byte) (HandshakeResult, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	res := HandshakeResult{
		MsgType:        types.MsgTypeHandshakeXXPsk2Resp,
		LocalSessionID: hs.localSessionID,
	}

	switch hs.nextStage {
	case stage1:
		tokenBytes, _, _, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return res, err
		}
		tokenID := string(tokenBytes)

		psk, exists := hs.admission.ConsumePSK(tokenID)
		if !exists {
			return res, fmt.Errorf("unknown invite: %q", tokenID)
		}

		if err = hs.SetPresharedKey(psk[:]); err != nil {
			return res, fmt.Errorf("failed to set psk: %w", err)
		}

		msg2, _, _, err := hs.WriteMessage(nil, hs.sigPub)
		if err != nil {
			return res, err
		}
		res.Msg = msg2

	case stage2:
		peerSig, csRecv, csSend, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return res, err
		}

		res.Session = newSession(hs.localSessionID, hs.PeerStatic(), csSend, csRecv, false) // we responded
		res.PeerStaticKey = hs.PeerStatic()
		res.PeerIdentityPub = peerSig

	default:
		return res, fmt.Errorf("unexpected stage: %v", hs.nextStage)
	}

	hs.nextStage++
	return res, nil
}

func genSessionID() (uint32, error) {
	b := make([]byte, sessionIDLength)
	if _, err := rand.Read(b); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

type rekeyManager struct {
	m  map[uint32]*time.Timer
	mu sync.Mutex
}

func newRekeyManager() *rekeyManager {
	return &rekeyManager{
		m: make(map[uint32]*time.Timer),
	}
}

func (m *rekeyManager) resetIfExists(peerSessionID uint32, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.m[peerSessionID]; ok {
		t.Reset(d)
	}
}
