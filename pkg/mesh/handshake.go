package mesh

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/invites"
)

var (
	_ handshake = (*handshakeIKInit)(nil)
	_ handshake = (*handshakeIKResp)(nil)
	_ handshake = (*handshakeXXPsk2Init)(nil)
	_ handshake = (*handshakeXXPsk2Resp)(nil)
)

var (
	ErrNoResolvableIP = errors.New("no resolvable IP from peer")
)

type HandshakeResult struct {
	Msg            []byte
	MsgType        MessageType
	LocalSessionID uint32
	Targets        []*net.UDPAddr

	// populated when handshake completes
	Session         *session
	PeerStaticKey   []byte
	PeerIdentityPub []byte
}

type handshake interface {
	Step(msg []byte) (HandshakeResult, error)
}

type handshakeStore struct {
	localIdentityPub []byte
	cs               *noise.CipherSuite
	invites          *invites.InviteStore
	localStaticKey   noise.DHKey
	st               map[uint32]handshake
	mu               sync.RWMutex
}

func newHandshakeStore(cs *noise.CipherSuite, invites *invites.InviteStore, localStaticKey noise.DHKey, localSigPub []byte) *handshakeStore {
	return &handshakeStore{
		cs:               cs,
		invites:          invites,
		localStaticKey:   localStaticKey,
		localIdentityPub: localSigPub,
		st:               make(map[uint32]handshake),
		mu:               sync.RWMutex{},
	}
}

func (st *handshakeStore) getOrCreate(peerSessionID, localSessionID uint32, tp MessageType) (handshake, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if hs, ok := st.st[peerSessionID]; ok {
		return hs, nil
	}

	var hs handshake
	var err error

	switch tp {
	case MessageTypeHandshakeIKResp, MessageTypeHandshakeXXPsk2Resp:
		// If we init a handshake, we set the handshake to the localSessionID.
		// These events will be the response to our init, so we swop out the local key for the peer one.
		var ok bool
		hs, ok = st.st[localSessionID]
		if !ok {
			return nil, nil
		}
		delete(st.st, localSessionID)
	case MessageTypeHandshakeIKInit:
		hs, err = newHandshakeIKResp(st.cs, st.localStaticKey)
		if err != nil {
			return nil, err
		}
	case MessageTypeHandshakeXXPsk2Init:
		hs, err = newHandshakeXXPsk2Resp(st.cs, st.invites, st.localStaticKey, st.localIdentityPub)
		if err != nil {
			return nil, err
		}
	}

	st.st[peerSessionID] = hs
	return hs, err
}

func (st *handshakeStore) initIK(peerStaticKey []byte, peerRawAddrs []string) (HandshakeResult, error) {
	hs, err := newHandshakeIKInit(st.cs, st.localStaticKey, peerStaticKey, peerRawAddrs)
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

func (st *handshakeStore) clear(peerSessionID uint32) {
	st.mu.Lock()
	defer st.mu.Unlock()

	delete(st.st, peerSessionID)
}

type handshakeStage int

const (
	stage1 handshakeStage = iota
	stage2
)

type handshakeIKInit struct {
	*noise.HandshakeState
	targets        []*net.UDPAddr
	nextStage      handshakeStage
	mu             sync.Mutex
	localSessionID uint32
}

func newHandshakeIKInit(cs *noise.CipherSuite, localStaticKey noise.DHKey, peerStaticKey []byte, peerRawAddrs []string) (*handshakeIKInit, error) {
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

	var targets []*net.UDPAddr
	for _, addr := range peerRawAddrs {
		if udpAddr, err := net.ResolveUDPAddr("udp", addr); err == nil {
			targets = append(targets, udpAddr)
		}
	}

	if len(targets) == 0 {
		return nil, ErrNoResolvableIP
	}

	localSessionID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	return &handshakeIKInit{
		HandshakeState: hs,
		localSessionID: localSessionID,
		targets:        targets,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeIKInit) Step(rcvMsg []byte) (HandshakeResult, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	res := HandshakeResult{
		LocalSessionID: hs.localSessionID,
		MsgType:        MessageTypeHandshakeIKInit,
	}

	switch hs.nextStage {
	case stage1:
		msg1, _, _, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return res, err
		}
		res.Msg = msg1
		res.Targets = hs.targets

	case stage2:
		_, csSend, csRecv, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return res, err
		}

		res.Session = newSession(nil, hs.PeerStatic(), csSend, csRecv)
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
	localSessionID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   *cs,
		Pattern:       noise.HandshakeIK,
		Prologue:      handshakePrologue,
		StaticKeypair: localStaticKey,
	})
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
		LocalSessionID: hs.localSessionID,
		MsgType:        MessageTypeHandshakeIKResp,
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
		res.Session = newSession(nil, hs.PeerStatic(), csSend, csRecv)
		res.PeerStaticKey = hs.PeerStatic()

	default:
		return res, fmt.Errorf("unexpected stage: %v", hs.nextStage)
	}

	hs.nextStage++
	return res, nil
}

type handshakeXXPsk2Init struct {
	*noise.HandshakeState
	sigPub         []byte
	candidates     []*net.UDPAddr
	tokenID        string
	nextStage      handshakeStage
	mu             sync.Mutex
	localSessionID uint32
}

func newHandshakeXXPsk2Init(cs *noise.CipherSuite, localStaticKey noise.DHKey, peerSigPub []byte, token *peerv1.Invite) (*handshakeXXPsk2Init, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:           *cs,
		Pattern:               noise.HandshakeXX,
		Initiator:             true,
		Prologue:              handshakePrologue,
		PresharedKeyPlacement: 2,
		PresharedKey:          token.Psk,
		StaticKeypair:         localStaticKey,
	})
	if err != nil {
		return nil, err
	}

	var candidates []*net.UDPAddr
	for _, a := range token.Addr {
		candidate, err := net.ResolveUDPAddr("udp", a)
		if err == nil {
			candidates = append(candidates, candidate)
		}
	}

	if len(candidates) == 0 {
		return nil, ErrNoResolvableIP
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
		candidates:     candidates,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeXXPsk2Init) Step(rcvMsg []byte) (HandshakeResult, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	res := HandshakeResult{
		LocalSessionID: hs.localSessionID,
		MsgType:        MessageTypeHandshakeXXPsk2Init,
	}

	switch hs.nextStage {
	case stage1:
		msg1, _, _, err := hs.WriteMessage(nil, []byte(hs.tokenID))
		if err != nil {
			return res, err
		}
		// Happy Eyeballs: Return all candidates as targets
		res.Msg = msg1
		res.Targets = hs.candidates

	case stage2:
		peerSig, _, _, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return res, err
		}

		msg3, csSend, csRecv, err := hs.WriteMessage(nil, []byte(hs.sigPub))
		if err != nil {
			return res, err
		}

		res.Msg = msg3
		res.Session = newSession(nil, hs.PeerStatic(), csSend, csRecv)
		res.PeerStaticKey = hs.PeerStatic()
		res.PeerIdentityPub = peerSig

	default:
		return res, fmt.Errorf("unexpected stage: %v", hs.nextStage)
	}

	hs.nextStage++
	return res, nil
}

type handshakeXXPsk2Resp struct {
	*noise.HandshakeState
	inviteStore    *invites.InviteStore
	sigPub         []byte
	nextStage      handshakeStage
	mu             sync.Mutex
	localSessionID uint32
}

func newHandshakeXXPsk2Resp(cs *noise.CipherSuite, invitesStore *invites.InviteStore, localStaticKey noise.DHKey, localSigPub []byte) (*handshakeXXPsk2Resp, error) {
	localSessionID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:           *cs,
		Pattern:               noise.HandshakeXX,
		Prologue:              handshakePrologue,
		PresharedKeyPlacement: 2,
		StaticKeypair:         localStaticKey,
	})
	if err != nil {
		return nil, err
	}

	return &handshakeXXPsk2Resp{
		HandshakeState: hs,
		inviteStore:    invitesStore,
		sigPub:         localSigPub,
		localSessionID: localSessionID,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeXXPsk2Resp) Step(rcvMsg []byte) (HandshakeResult, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	res := HandshakeResult{
		LocalSessionID: hs.localSessionID,
		MsgType:        MessageTypeHandshakeXXPsk2Resp,
	}

	switch hs.nextStage {
	case stage1:
		tokenBytes, _, _, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return res, err
		}
		tokenID := invites.InviteID(tokenBytes)

		// delete regardless of the outcome of the handshake
		defer hs.inviteStore.DeleteInvite(tokenID)

		inv, exists := hs.inviteStore.GetInvite(tokenID)
		if !exists {
			return res, fmt.Errorf("unknown invite: %q", tokenID)
		}

		if err = hs.SetPresharedKey(inv.Psk); err != nil {
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

		res.Session = newSession(nil, hs.PeerStatic(), csSend, csRecv)
		res.PeerStaticKey = hs.PeerStatic()
		res.PeerIdentityPub = peerSig

	default:
		return res, fmt.Errorf("unexpected stage: %v", hs.nextStage)
	}

	hs.nextStage++
	return res, nil
}

func genSessionID() (uint32, error) {
	b := make([]byte, 4)
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

// set should only be called if the node is known to be the initiator of the connection with
// the given peerStaticKey.
func (m *rekeyManager) set(peerSessionID uint32, peerStaticKey []byte, t *time.Timer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if oldT, ok := m.m[peerSessionID]; ok {
		oldT.Stop()
	}

	m.m[peerSessionID] = t
}

func (m *rekeyManager) resetIfExists(peerSessionID uint32, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.m[peerSessionID]; ok {
		t.Reset(d)
	}
}
