package mesh

import (
	"crypto/rand"
	"encoding/binary"
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

type handshake interface {
	progress(conn *UDPConn, msg []byte, peerUDPAddr *net.UDPAddr, peerSessionID uint32) (*session, []byte, error)
}

type handshakeStore struct {
	cs             *noise.CipherSuite
	invites        *invites.InviteStore
	localStaticKey *noise.DHKey
	st             map[uint32]handshake
	mu             sync.RWMutex
}

func newHandshakeStore(cs *noise.CipherSuite, invites *invites.InviteStore, localStaticKey *noise.DHKey) *handshakeStore {
	return &handshakeStore{
		cs:             cs,
		invites:        invites,
		localStaticKey: localStaticKey,
		st:             make(map[uint32]handshake),
		mu:             sync.RWMutex{},
	}
}

func (st *handshakeStore) get(tp messageType, peerSessionID, localSessionID uint32) (handshake, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if hs, ok := st.st[peerSessionID]; ok {
		return hs, nil
	}

	var hs handshake
	var err error
	switch tp {
	case messageTypeHandshakeIKResp, messageTypeHandshakeXXPsk2Resp:
		// If we init a handshake, we set the handshake to the localSessionID.
		// These events will be the response to our init, we swop out the local key for the peer one.
		// Under normal circumstances, we likely won't need to use the updated key for further lookups
		// from received packets, but we keep it around for a potential future that supports full idempotency
		// and retries.
		var ok bool
		hs, ok = st.st[localSessionID]
		if !ok {
			return nil, nil
		}
		delete(st.st, localSessionID)
	case messageTypeHandshakeIKInit:
		hs, err = newHandshakeIKResp(st.cs, st.localStaticKey, localSessionID)
		if err != nil {
			return nil, err
		}
	case messageTypeHandshakeXXPsk2Init:
		hs, err = newHandshakeXXPsk2Resp(st.cs, st.invites, st.localStaticKey, localSessionID)
		if err != nil {
			return nil, err
		}
	}

	st.st[peerSessionID] = hs
	return hs, err
}

func (st *handshakeStore) initIK(conn *UDPConn, peerStaticKey []byte, peerRawAddress string) error {
	hs, err := newHandshakeIKInit(st.cs, st.localStaticKey, peerStaticKey, peerRawAddress)
	if err != nil {
		return err
	}

	st.mu.Lock()
	st.st[hs.localSessionID] = hs
	st.mu.Unlock()

	if _, _, err := hs.progress(conn, nil, nil, 0); err != nil {
		return fmt.Errorf("failed to start IK handshake: %w", err)
	}

	return nil
}

func (st *handshakeStore) initXXPsk2(conn *UDPConn, token *peerv1.Invite) error {
	hs, err := newHandshakeXXPsk2Init(st.cs, st.localStaticKey, token)
	if err != nil {
		return err
	}

	st.mu.Lock()
	st.st[hs.localSessionID] = hs
	st.mu.Unlock()

	if _, _, err := hs.progress(conn, nil, nil, 0); err != nil {
		return fmt.Errorf("failed to start XXpsk2 handshake: %w", err)
	}

	return nil
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
	peerUDPAddr    *net.UDPAddr
	peerRawAddr    string
	nextStage      handshakeStage
	mu             sync.Mutex
	localSessionID uint32
}

func newHandshakeIKInit(cs *noise.CipherSuite, localStaticKey *noise.DHKey, peerStaticKey []byte, peerRawAddr string) (*handshakeIKInit, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   *cs,
		Pattern:       noise.HandshakeIK,
		Initiator:     true,
		Prologue:      handshakePrologue,
		StaticKeypair: *localStaticKey,
		PeerStatic:    peerStaticKey,
	})
	if err != nil {
		return nil, err
	}

	peerUDPAddr, err := net.ResolveUDPAddr("udp", peerRawAddr)
	if err != nil {
		return nil, err
	}

	senderID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	return &handshakeIKInit{
		HandshakeState: hs,
		localSessionID: senderID,
		peerRawAddr:    peerRawAddr,
		peerUDPAddr:    peerUDPAddr,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeIKInit) progress(conn *UDPConn, rcvMsg []byte, _ *net.UDPAddr, _ uint32) (*session, []byte, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	switch hs.nextStage {
	case stage1:
		msg1, _, _, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return nil, nil, err
		}
		if err := write(conn, hs.peerUDPAddr, messageTypeHandshakeIKInit, hs.localSessionID, 0, msg1); err != nil {
			return nil, nil, err
		}
	case stage2:
		_, csSend, csRecv, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return nil, nil, err
		}

		return newSession(hs.peerUDPAddr, csSend, csRecv), hs.PeerStatic(), nil
	default:
		return nil, nil, fmt.Errorf("unexpected mesh.handshakeStage: %#v", hs.nextStage)
	}

	hs.nextStage++

	return nil, nil, nil
}

type handshakeIKResp struct {
	*noise.HandshakeState
	localSessionID uint32
	nextStage      handshakeStage
	mu             sync.Mutex
}

func newHandshakeIKResp(cs *noise.CipherSuite, localStaticKey *noise.DHKey, localSessionID uint32) (*handshakeIKResp, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   *cs,
		Pattern:       noise.HandshakeIK,
		Prologue:      handshakePrologue,
		StaticKeypair: *localStaticKey,
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

func (hs *handshakeIKResp) progress(conn *UDPConn, rcvMsg []byte, peerUDPAddr *net.UDPAddr, peerSessionID uint32) (*session, []byte, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	switch hs.nextStage {
	case stage1:
		if _, _, _, err := hs.ReadMessage(nil, rcvMsg); err != nil {
			return nil, nil, err
		}

		msg2, csRecv, csSend, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return nil, nil, err
		}

		if err := write(conn, peerUDPAddr, messageTypeHandshakeIKResp, peerSessionID, hs.localSessionID, msg2); err != nil {
			return nil, nil, err
		}

		return newSession(peerUDPAddr, csSend, csRecv), hs.PeerStatic(), nil
	default:
		return nil, nil, fmt.Errorf("unexpected mesh.handshakeStage: %#v", hs.nextStage)
	}
}

type handshakeXXPsk2Init struct {
	*noise.HandshakeState
	peerUDPAddr    *net.UDPAddr
	tokenID        string
	nextStage      handshakeStage
	mu             sync.Mutex
	localSessionID uint32
}

func newHandshakeXXPsk2Init(cs *noise.CipherSuite, localStaticKey *noise.DHKey, token *peerv1.Invite) (*handshakeXXPsk2Init, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:           *cs,
		Pattern:               noise.HandshakeXX,
		Initiator:             true,
		Prologue:              handshakePrologue,
		PresharedKeyPlacement: 2,
		PresharedKey:          token.Psk,
		StaticKeypair:         *localStaticKey,
	})
	if err != nil {
		return nil, err
	}

	peerUDPAddr, err := net.ResolveUDPAddr("udp", token.Addr)
	if err != nil {
		return nil, err
	}

	senderID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	return &handshakeXXPsk2Init{
		HandshakeState: hs,
		localSessionID: senderID,
		tokenID:        token.Id,
		peerUDPAddr:    peerUDPAddr,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeXXPsk2Init) progress(conn *UDPConn, rcvMsg []byte, _ *net.UDPAddr, peerSessionID uint32) (*session, []byte, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	switch hs.nextStage {
	case stage1:
		msg1, _, _, err := hs.WriteMessage(nil, []byte(hs.tokenID))
		if err != nil {
			return nil, nil, err
		}
		if err := write(conn, hs.peerUDPAddr, messageTypeHandshakeXXPsk2Init, hs.localSessionID, 0, msg1); err != nil {
			return nil, nil, err
		}
	case stage2:
		if _, _, _, err := hs.ReadMessage(nil, rcvMsg); err != nil {
			return nil, nil, err
		}

		msg3, csSend, csRecv, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return nil, nil, err
		}
		if err := write(conn, hs.peerUDPAddr, messageTypeHandshakeXXPsk2Init, hs.localSessionID, peerSessionID, msg3); err != nil {
			return nil, nil, err
		}

		return newSession(hs.peerUDPAddr, csSend, csRecv), hs.PeerStatic(), nil
	default:
		return nil, nil, fmt.Errorf("unexpected mesh.handshakeStage: %#v", hs.nextStage)
	}

	hs.nextStage++

	return nil, nil, nil
}

type handshakeXXPsk2Resp struct {
	*noise.HandshakeState
	inviteStore    *invites.InviteStore
	peerUDPAddr    *net.UDPAddr
	buf            []byte
	nextStage      handshakeStage
	mu             sync.Mutex
	localSessionID uint32
}

func newHandshakeXXPsk2Resp(cs *noise.CipherSuite, invitesStore *invites.InviteStore, localStaticKey *noise.DHKey, localSessionID uint32) (*handshakeXXPsk2Resp, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:           *cs,
		Pattern:               noise.HandshakeXX,
		Prologue:              handshakePrologue,
		PresharedKeyPlacement: 2,
		StaticKeypair:         *localStaticKey,
	})
	if err != nil {
		return nil, err
	}

	return &handshakeXXPsk2Resp{
		HandshakeState: hs,
		inviteStore:    invitesStore,
		localSessionID: localSessionID,
		nextStage:      stage1,
		buf:            make([]byte, 2048),
	}, nil
}

func (hs *handshakeXXPsk2Resp) progress(conn *UDPConn, rcvMsg []byte, peerUDPAddr *net.UDPAddr, peerSessionID uint32) (*session, []byte, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	switch hs.nextStage {
	case stage1:
		hs.peerUDPAddr = peerUDPAddr
		tokenBytes, _, _, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return nil, nil, err
		}
		tokenID := invites.InviteID(tokenBytes)

		// delete regardless of the outcome of the handshake
		defer hs.inviteStore.DeleteInvite(tokenID)

		inv, exists := hs.inviteStore.GetInvite(tokenID)
		if !exists {
			return nil, nil, fmt.Errorf("unknown invite: %q", tokenID)
		}

		if err = hs.SetPresharedKey(inv.Psk); err != nil {
			return nil, nil, fmt.Errorf("failed to set psk: %w", err)
		}

		msg2, _, _, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return nil, nil, err
		}
		if err := write(conn, peerUDPAddr, messageTypeHandshakeXXPsk2Resp, peerSessionID, hs.localSessionID, msg2); err != nil {
			return nil, nil, err
		}

	case stage2:
		_, csRecv, csSend, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return nil, nil, err
		}

		return newSession(hs.peerUDPAddr, csSend, csRecv), hs.PeerStatic(), nil
	default:
		return nil, nil, fmt.Errorf("unexpected mesh.handshakeStage: %#v", hs.nextStage)
	}

	hs.nextStage++

	return nil, nil, nil
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
// the given peerStaticKey. This is useful for differentiating between rekeys triggered by the
// default interval and forced rekeys to avoid nonce expiration (which can be triggered on the
// responder side).
func (m *rekeyManager) set(peerSessionID uint32, peerStaticKey []byte, t *time.Timer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if oldT, ok := m.m[peerSessionID]; ok {
		oldT.Stop()
	}

	m.m[peerSessionID] = t
}

// resetIfExists only resets the timer if one is present. This will only be called if the responder
// (peer) needs to force a rekey to avoid nonce expiration.
func (m *rekeyManager) resetIfExists(peerSessionID uint32, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.m[peerSessionID]; ok {
		t.Reset(d)
	}
}
