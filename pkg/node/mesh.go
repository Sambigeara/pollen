package node

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/peers"
	"go.uber.org/zap"
)

type noiseMesh struct {
	log            *zap.SugaredLogger
	addr           string
	peers          *peers.PeerStore
	localStaticKey noise.DHKey
	noiseCS        noise.CipherSuite
	// TODO(saml) noiseConns should be a local, non-gossiped CRDT (we can update the conn connection as both the intiator
	// or the responder, but should refresh to the latest each time)
	noiseConns map[string]*noiseConn
	mu         sync.RWMutex
}

func newNoiseMesh(peers *peers.PeerStore, cs noise.CipherSuite, staticKey noise.DHKey, addr string) (*noiseMesh, error) {
	log := zap.S().Named("mesh")

	return &noiseMesh{
		log:            log,
		addr:           addr,
		peers:          peers,
		localStaticKey: staticKey,
		noiseCS:        cs,
		noiseConns:     make(map[string]*noiseConn),
	}, nil
}

func (m *noiseMesh) updateConn(peerStatic string, conn *noiseConn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.noiseConns[peerStatic]; ok && existing != nil {
		existing.Close()
	}

	m.noiseConns[peerStatic] = conn
}

type noiseConn struct {
	net.Conn
	peerStatic []byte
	// TODO(saml) implement send.Rekey() on both every 1<<20 bytes or 1000 messages or whatever Wireguards standard is
	send *noise.CipherState
	recv *noise.CipherState
}

func writeHsPkt(conn net.Conn, b []byte) error {
	var lenb [2]byte
	binary.BigEndian.PutUint16(lenb[:], uint16(len(b)))
	if _, err := conn.Write(lenb[:]); err != nil {
		return err
	}
	_, err := conn.Write(b)
	return err
}

func readHsPkt(conn net.Conn) ([]byte, error) {
	var lenb [2]byte
	if _, err := io.ReadFull(conn, lenb[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(lenb[:]))
	b := make([]byte, n)
	_, err := io.ReadFull(conn, b)
	return b, err
}

func (m *noiseMesh) establishPublishConns(token *peerv1.Invite) {
	for _, k := range m.peers.GetAllKnown() {
		conn, err := net.Dial("tcp", k.Addr)
		if err != nil {
			m.log.Debugf("Failed to connect to %s: %v", k.Addr, err)
			continue
		}

		noiseConn, err := m.performInitiatorNoiseHandshakeIK(conn, k.StaticKey)
		if err != nil {
			m.log.Errorf("noise IK handshake failed for %s: %v", k.Addr, err)
			conn.Close()
			continue
		}

		m.updateConn(string(k.StaticKey), noiseConn)
	}

	if token != nil {
		conn, err := net.Dial("tcp", token.Addr)
		if err != nil {
			m.log.Errorf("Failed to connect to %s: %v", token.Addr, err)
			return
		}

		noiseConn, err := m.performInitiatorNoiseHandshakeXXPsk2(conn, token)
		if err != nil {
			m.log.Errorf("noise XXpsk2 handshake failed for %s: %v", token.Addr, err)
			conn.Close()
			return
		}

		m.updateConn(string(noiseConn.peerStatic), noiseConn)
	}
}

func (m *noiseMesh) performInitiatorNoiseHandshakeXXPsk2(conn net.Conn, token *peerv1.Invite) (*noiseConn, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:           m.noiseCS,
		Pattern:               noise.HandshakeXX,
		Initiator:             true,
		Prologue:              handshakePrologue,
		PresharedKeyPlacement: 2,
		PresharedKey:          token.Psk,
		StaticKeypair:         m.localStaticKey,
	})
	if err != nil {
		return nil, err
	}

	msg1, _, _, err := hs.WriteMessage(nil, []byte(token.Id))
	if err != nil {
		return nil, err
	}
	if err := writeHsPkt(conn, msg1); err != nil {
		return nil, err
	}

	msg2, err := readHsPkt(conn)
	if err != nil {
		return nil, err
	}
	if _, _, _, err := hs.ReadMessage(nil, msg2); err != nil {
		return nil, err
	}

	msg3, csSend, csRecv, err := hs.WriteMessage(nil, []byte(m.addr))
	if err != nil {
		return nil, err
	}
	if err := writeHsPkt(conn, msg3); err != nil {
		return nil, err
	}

	m.peers.Add(&peerv1.Known{
		StaticKey: hs.PeerStatic(),
		Addr:      token.Addr,
	})

	m.log.Infof("completed XXpsk2 initiator handshake with responder addr: %s", token.Addr)

	return &noiseConn{
		Conn:       conn,
		peerStatic: hs.PeerStatic(),
		send:       csSend,
		recv:       csRecv,
	}, nil
}

func (m *noiseMesh) performInitiatorNoiseHandshakeIK(conn net.Conn, peerStaticKey []byte) (*noiseConn, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   m.noiseCS,
		Pattern:       noise.HandshakeIK,
		Initiator:     true,
		Prologue:      handshakePrologue,
		StaticKeypair: m.localStaticKey,
		PeerStatic:    peerStaticKey,
	})
	if err != nil {
		return nil, err
	}

	msg1, _, _, err := hs.WriteMessage(nil, []byte(m.addr))
	if err != nil {
		return nil, err
	}
	if err := writeHsPkt(conn, msg1); err != nil {
		return nil, err
	}

	msg2, err := readHsPkt(conn)
	if err != nil {
		return nil, err
	}
	_, csSend, csRecv, err := hs.ReadMessage(nil, msg2)
	if err != nil {
		return nil, err
	}

	if peer, ok := m.peers.Get(hs.PeerStatic()); ok {
		m.log.Infof("completed IK initiator handshake with responder (from cache): %s", peer.Addr)
	}

	return &noiseConn{
		Conn:       conn,
		peerStatic: hs.PeerStatic(),
		send:       csSend,
		recv:       csRecv,
	}, nil
}

func (m *noiseMesh) performResponderNoiseHandshake(conn net.Conn) (*noiseConn, error) {
	msg1, err := readHsPkt(conn)
	if err != nil {
		return nil, err
	}

	ns, err := m.performResponderNoiseHandshakeIK(conn, msg1)
	if err != nil {
		// TODO(saml) be more specific with err types (we should only retry with XXPsk2 if the initial frame read failed, I think)
		return m.performResponderNoiseHandshakeXXPsk2(conn, msg1)
	}

	return ns, err
}

func (m *noiseMesh) performResponderNoiseHandshakeXXPsk2(conn net.Conn, msg1 []byte) (*noiseConn, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:           m.noiseCS,
		Pattern:               noise.HandshakeXX,
		Prologue:              handshakePrologue,
		PresharedKeyPlacement: 2,
		StaticKeypair:         m.localStaticKey,
	})
	if err != nil {
		return nil, err
	}

	tokenIDBytes, _, _, err := hs.ReadMessage(nil, msg1)
	if err != nil {
		return nil, err
	}
	tokenID := peers.InviteID(tokenIDBytes)

	// delete regardless of the outcome of the handshake
	defer m.peers.DeleteInvite(tokenID)

	inv, exists := m.peers.GetInvite(tokenID)
	if !exists {
		return nil, fmt.Errorf("unknown invite: %q", tokenID)
	}

	if err = hs.SetPresharedKey(inv.Psk); err != nil {
		return nil, fmt.Errorf("failed to set psk: %v", err)
	}

	msg2, _, _, err := hs.WriteMessage(nil, nil)
	if err != nil {
		return nil, err
	}
	if err := writeHsPkt(conn, msg2); err != nil {
		return nil, err
	}

	msg3, err := readHsPkt(conn)
	if err != nil {
		return nil, err
	}
	addrBytes, csRecv, csSend, err := hs.ReadMessage(nil, msg3)
	if err != nil {
		return nil, err
	}
	addr := string(addrBytes)

	m.peers.PromoteToPeer(hs.PeerStatic(), tokenID, addr)
	m.log.Infof("completed XXpsk2 responder handshake with initiator: %s", addr)

	return &noiseConn{
		Conn:       conn,
		peerStatic: hs.PeerStatic(),
		send:       csSend,
		recv:       csRecv,
	}, nil
}

func (m *noiseMesh) performResponderNoiseHandshakeIK(conn net.Conn, msg1 []byte) (*noiseConn, error) {
	hs, _ := noise.NewHandshakeState(noise.Config{
		CipherSuite:   m.noiseCS,
		Pattern:       noise.HandshakeIK,
		Prologue:      handshakePrologue,
		StaticKeypair: m.localStaticKey,
	})

	addrBytes, _, _, err := hs.ReadMessage(nil, msg1)
	if err != nil {
		return nil, err
	}
	addr := string(addrBytes)

	// TODO(saml) addr this needs to feed in to local state to ensure we always have the latest address for a given static key

	msg2, csRecv, csSend, err := hs.WriteMessage(nil, nil)
	if err != nil {
		return nil, err
	}
	if err := writeHsPkt(conn, msg2); err != nil {
		return nil, err
	}

	m.log.Infof("completed IK responder handshake with initiator (from cache): %s", addr)

	return &noiseConn{
		Conn:       conn,
		peerStatic: hs.PeerStatic(),
		send:       csSend,
		recv:       csRecv,
	}, nil
}

func (m *noiseMesh) shutdown(ctx context.Context) error {
	if err := m.peers.Save(); err != nil {
		return err
	}

	return ctx.Err()
}

// BELOW ME IS AI GENERATED STUFF
// ...

// length-prefixed write (same helpers you already have)
func writeAll(c net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := c.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func writeFrame(c net.Conn, b []byte) error {
	var hdr [2]byte
	binary.BigEndian.PutUint16(hdr[:], uint16(len(b)))
	if err := writeAll(c, hdr[:]); err != nil {
		return err
	}
	return writeAll(c, b)
}

func readFrame(c net.Conn) ([]byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(hdr[:]))
	buf := make([]byte, n)
	_, err := io.ReadFull(c, buf)
	return buf, err
}

// Encrypt + send one application packet
func (s *noiseConn) WriteSecure(p []byte) error {
	ct, err := s.send.Encrypt(nil, nil, p) // aad=nil
	if err != nil {
		return err
	}
	return writeFrame(s.Conn, ct)
}

// Read + decrypt one application packet
func (s *noiseConn) ReadSecure() ([]byte, error) {
	ct, err := readFrame(s.Conn)
	if err != nil {
		return nil, err
	}
	pt, err := s.recv.Decrypt(nil, nil, ct) // aad=nil
	if err != nil {
		return nil, err
	}
	return pt, nil
}
