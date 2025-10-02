package mesh

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/peers"
)

var (
	_ handshake = (*handshakeIKInit)(nil)
	_ handshake = (*handshakeIKResp)(nil)
	_ handshake = (*handshakeXXPsk2Init)(nil)
	_ handshake = (*handshakeXXPsk2Resp)(nil)
)

type handshake interface {
	progress(conn *net.UDPConn, msg []byte, peerUDPAddr *net.UDPAddr) (*noiseConn, error)
}

type handshakeKind uint32

const (
	handshakeKindXXPsk2Init handshakeKind = iota
	handshakeKindXXPsk2Resp
	handshakeKindIKInit
	handshakeKindIKResp
)

type handshakeStore struct {
	cs             *noise.CipherSuite
	peers          *peers.PeerStore
	localStaticKey *noise.DHKey
	st             map[uint32]handshake
	mu             sync.RWMutex
}

func newHandshakeStore(cs *noise.CipherSuite, peersStore *peers.PeerStore, localStaticKey *noise.DHKey) *handshakeStore {
	return &handshakeStore{
		cs:             cs,
		peers:          peersStore,
		localStaticKey: localStaticKey,
		st:             make(map[uint32]handshake),
		mu:             sync.RWMutex{},
	}
}

func (st *handshakeStore) get(k handshakeKind, senderID uint32) (handshake, error) {
	st.mu.RLock()
	hs, ok := st.st[senderID]
	st.mu.RUnlock()

	if !ok {
		st.mu.Lock()
		defer st.mu.Unlock()

		switch k {
		case handshakeKindIKInit:
			var err error
			hs, err = newHandshakeIKResp(st.cs, st.localStaticKey, senderID)
			if err != nil {
				return nil, err
			}
		case handshakeKindXXPsk2Init:
			var err error
			hs, err = newHandshakeXXPsk2Resp(st.cs, st.localStaticKey, senderID, st.peers)
			if err != nil {
				return nil, err
			}
		}
	}

	st.st[senderID] = hs

	return hs, nil
}

func (st *handshakeStore) initIK(peerStaticKey []byte) (handshake, error) {
	p, ok := st.peers.Get(peerStaticKey)
	if !ok {
		return nil, errors.New("static key not recognised")
	}

	hs, err := newHandshakeIKInit(st.cs, st.localStaticKey, peerStaticKey, p.Addr)
	if err != nil {
		return nil, err
	}

	st.mu.Lock()
	st.st[hs.sessionID] = hs
	st.mu.Unlock()

	return hs, nil
}

func (st *handshakeStore) initXXPsk2(token *peerv1.Invite) (handshake, error) {
	hs, err := newHandshakeXXPsk2Init(st.cs, st.localStaticKey, token, st.peers)
	if err != nil {
		return nil, err
	}

	st.mu.Lock()
	st.st[hs.sessionID] = hs
	st.mu.Unlock()

	return hs, nil
}

func (st *handshakeStore) clear(senderID uint32) {
	st.mu.Lock()
	defer st.mu.Unlock()

	delete(st.st, senderID)
}

type handshakeStage int

const (
	stage1 handshakeStage = iota
	stage2
)

type handshakeIKInit struct {
	*noise.HandshakeState
	peerUDPAddr *net.UDPAddr
	peerRawAddr string
	nextStage   handshakeStage
	mu          sync.Mutex
	sessionID   uint32
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

	sessID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	return &handshakeIKInit{
		HandshakeState: hs,
		sessionID:      sessID,
		peerRawAddr:    peerRawAddr,
		peerUDPAddr:    peerUDPAddr,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeIKInit) progress(conn *net.UDPConn, rcvMsg []byte, _ *net.UDPAddr) (*noiseConn, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	switch hs.nextStage {
	case stage1:
		msg1, _, _, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return nil, err
		}
		if err := write(conn, hs.peerUDPAddr, handshakeKindIKInit, hs.sessionID, msg1); err != nil {
			return nil, err
		}
	case stage2:
		_, csSend, csRecv, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return nil, err
		}

		return &noiseConn{
			peerStatic: hs.PeerStatic(),
			send:       csSend,
			recv:       csRecv,
		}, nil
	default:
		return nil, fmt.Errorf("unexpected mesh.handshakeStage: %#v", hs.nextStage)
	}

	hs.nextStage++

	return nil, nil
}

type handshakeIKResp struct {
	*noise.HandshakeState
	sessionID uint32
	nextStage handshakeStage
	mu        sync.Mutex
}

func newHandshakeIKResp(cs *noise.CipherSuite, localStaticKey *noise.DHKey, senderID uint32) (*handshakeIKResp, error) {
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
		sessionID:      senderID,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeIKResp) progress(conn *net.UDPConn, rcvMsg []byte, peerUDPAddr *net.UDPAddr) (*noiseConn, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	switch hs.nextStage {
	case stage1:
		if _, _, _, err := hs.ReadMessage(nil, rcvMsg); err != nil {
			return nil, err
		}

		msg2, csRecv, csSend, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return nil, err
		}

		if err := write(conn, peerUDPAddr, handshakeKindIKResp, hs.sessionID, msg2); err != nil {
			return nil, err
		}

		return &noiseConn{
			peerStatic: hs.PeerStatic(),
			send:       csSend,
			recv:       csRecv,
		}, nil
	default:
		return nil, fmt.Errorf("unexpected mesh.handshakeStage: %#v", hs.nextStage)
	}
}

type handshakeXXPsk2Init struct {
	*noise.HandshakeState
	peersStore  *peers.PeerStore
	peerUDPAddr *net.UDPAddr
	tokenID     string
	nextStage   handshakeStage
	mu          sync.Mutex
	sessionID   uint32
}

func newHandshakeXXPsk2Init(cs *noise.CipherSuite, localStaticKey *noise.DHKey, token *peerv1.Invite, peersStore *peers.PeerStore) (*handshakeXXPsk2Init, error) {
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

	sessID, err := genSessionID()
	if err != nil {
		return nil, err
	}

	return &handshakeXXPsk2Init{
		HandshakeState: hs,
		sessionID:      sessID,
		peersStore:     peersStore,
		tokenID:        token.Id,
		peerUDPAddr:    peerUDPAddr,
		nextStage:      stage1,
	}, nil
}

func (hs *handshakeXXPsk2Init) progress(conn *net.UDPConn, rcvMsg []byte, _ *net.UDPAddr) (*noiseConn, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	switch hs.nextStage {
	case stage1:
		msg1, _, _, err := hs.WriteMessage(nil, []byte(hs.tokenID))
		if err != nil {
			return nil, err
		}
		if err := write(conn, hs.peerUDPAddr, handshakeKindXXPsk2Init, hs.sessionID, msg1); err != nil {
			return nil, err
		}
	case stage2:
		if _, _, _, err := hs.ReadMessage(nil, rcvMsg); err != nil {
			return nil, err
		}

		msg3, csSend, csRecv, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return nil, err
		}
		if err := write(conn, hs.peerUDPAddr, handshakeKindXXPsk2Init, hs.sessionID, msg3); err != nil {
			return nil, err
		}

		hs.peersStore.Add(&peerv1.Known{
			StaticKey: hs.PeerStatic(),
			Addr:      hs.peerUDPAddr.String(),
		})

		return &noiseConn{
			peerStatic: hs.PeerStatic(),
			send:       csSend,
			recv:       csRecv,
		}, nil
	default:
		return nil, fmt.Errorf("unexpected mesh.handshakeStage: %#v", hs.nextStage)
	}

	hs.nextStage++

	return nil, nil
}

type handshakeXXPsk2Resp struct {
	*noise.HandshakeState
	peersStore  *peers.PeerStore
	peerUDPAddr *net.UDPAddr
	buf         []byte
	nextStage   handshakeStage
	mu          sync.Mutex
	sessionID   uint32
}

func newHandshakeXXPsk2Resp(cs *noise.CipherSuite, localStaticKey *noise.DHKey, senderID uint32, peersStore *peers.PeerStore) (*handshakeXXPsk2Resp, error) {
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
		sessionID:      senderID,
		peersStore:     peersStore,
		nextStage:      stage1,
		buf:            make([]byte, 2048),
	}, nil
}

func (hs *handshakeXXPsk2Resp) progress(conn *net.UDPConn, rcvMsg []byte, peerUDPAddr *net.UDPAddr) (*noiseConn, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	switch hs.nextStage {
	case stage1:
		hs.peerUDPAddr = peerUDPAddr
		tokenBytes, _, _, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return nil, err
		}
		tokenID := peers.InviteID(tokenBytes)

		// delete regardless of the outcome of the handshake
		defer hs.peersStore.DeleteInvite(tokenID)

		inv, exists := hs.peersStore.GetInvite(tokenID)
		if !exists {
			return nil, fmt.Errorf("unknown invite: %q", tokenID)
		}

		if err = hs.SetPresharedKey(inv.Psk); err != nil {
			return nil, fmt.Errorf("failed to set psk: %w", err)
		}

		msg2, _, _, err := hs.WriteMessage(nil, nil)
		if err != nil {
			return nil, err
		}
		if err := write(conn, peerUDPAddr, handshakeKindXXPsk2Resp, hs.sessionID, msg2); err != nil {
			return nil, err
		}

	case stage2:
		_, csRecv, csSend, err := hs.ReadMessage(nil, rcvMsg)
		if err != nil {
			return nil, err
		}

		hs.peersStore.PromoteToPeer(hs.PeerStatic(), hs.peerUDPAddr.String())

		// TODO(saml) need to delete the invite regardless of success (maybe index differently in peersStore?)

		return &noiseConn{
			peerStatic: hs.PeerStatic(),
			send:       csSend,
			recv:       csRecv,
		}, nil
	default:
		return nil, fmt.Errorf("unexpected mesh.handshakeStage: %#v", hs.nextStage)
	}

	hs.nextStage++

	return nil, nil
}

func genSessionID() (uint32, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

type datagram struct {
	senderUDPAddr *net.UDPAddr
	msg           []byte
	kind          handshakeKind
	senderID      uint32
}

func write(conn *net.UDPConn, addr *net.UDPAddr, kind handshakeKind, senderID uint32, msg []byte) error {
	datagram := make([]byte, 8+len(msg))
	binary.BigEndian.PutUint32(datagram[:4], uint32(kind))
	binary.BigEndian.PutUint32(datagram[4:8], senderID)
	copy(datagram[8:], msg)

	_, err := conn.WriteToUDP(datagram, addr)
	return err
}

func read(conn *net.UDPConn, buf []byte) (*datagram, error) {
	if buf == nil {
		buf = make([]byte, 2048)
	}
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}
	if n < 8 { // 4 bytes kind + 4 bytes senderID
		return nil, fmt.Errorf("handshake frame too short: %d", n)
	}

	return &datagram{
		kind:          handshakeKind(binary.BigEndian.Uint32(buf[:4])),
		senderID:      binary.BigEndian.Uint32(buf[4:8]),
		senderUDPAddr: addr,
		msg:           append([]byte(nil), buf[8:n]...), // copy to decouple from shared buffer
	}, nil
}
