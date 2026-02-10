package sock

import (
	"bytes"
	"testing"

	"github.com/sambigeara/pollen/pkg/types"
)

// makeTestKey creates a 32-byte key filled with the given byte value.
func makeTestKey(fill byte) []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = fill
	}
	return key
}

// makeTestSession creates a minimal session for testing tie-breaking logic.
// send/recv cipher states are nil since we're only testing the store logic.
func makeTestSession(localSessionID uint32, peerNoiseKey []byte, isInitiator bool) *session {
	return &session{
		localSessionID: localSessionID,
		peerKey:        types.PeerKeyFromBytes(peerNoiseKey),
		isInitiator:    isInitiator,
	}
}

// TestSessionStore_TieBreaking_SmallerKeyIsInitiator verifies that when the
// local node has a smaller key (should be initiator), an initiator session
// is kept over a later responder session for the same peer.
func TestSessionStore_TieBreaking_SmallerKeyIsInitiator(t *testing.T) {
	smallKey := makeTestKey(0x01) // local key - smaller
	largeKey := makeTestKey(0xFF) // peer key - larger

	// Sanity check our test setup
	if bytes.Compare(smallKey, largeKey) >= 0 {
		t.Fatal("test setup error: smallKey should be less than largeKey")
	}

	store := newSessionStore(smallKey)

	// First: set an initiator session (correct role since local has smaller key)
	initiatorSess := makeTestSession(100, largeKey, true)
	store.set(initiatorSess)

	// Second: try to set a responder session (wrong role)
	responderSess := makeTestSession(200, largeKey, false)
	store.set(responderSess)

	// Verify: initiator session should be kept in byPeer
	peerKey := types.PeerKeyFromBytes(largeKey)
	gotSess, ok := store.getByPeer(peerKey)
	if !ok {
		t.Fatal("expected session to exist in byPeer")
	}
	if gotSess.localSessionID != 100 {
		t.Errorf("expected initiator session (ID=100) in byPeer, got ID=%d", gotSess.localSessionID)
	}
	if !gotSess.isInitiator {
		t.Error("expected session in byPeer to be initiator")
	}

	// Verify: both sessions should be accessible by localID
	if _, ok := store.getByLocalID(100); !ok {
		t.Error("initiator session (ID=100) should be accessible by localID")
	}
	if _, ok := store.getByLocalID(200); !ok {
		t.Error("responder session (ID=200) should be accessible by localID (for transition)")
	}
}

// TestSessionStore_TieBreaking_LargerKeyIsResponder verifies that when the
// local node has a larger key (should be responder), a responder session
// is kept over a later initiator session for the same peer.
func TestSessionStore_TieBreaking_LargerKeyIsResponder(t *testing.T) {
	largeKey := makeTestKey(0xFF) // local key - larger
	smallKey := makeTestKey(0x01) // peer key - smaller

	// Sanity check our test setup
	if bytes.Compare(largeKey, smallKey) <= 0 {
		t.Fatal("test setup error: largeKey should be greater than smallKey")
	}

	store := newSessionStore(largeKey)

	// First: set a responder session (correct role since local has larger key)
	responderSess := makeTestSession(100, smallKey, false)
	store.set(responderSess)

	// Second: try to set an initiator session (wrong role)
	initiatorSess := makeTestSession(200, smallKey, true)
	store.set(initiatorSess)

	// Verify: responder session should be kept in byPeer
	peerKey := types.PeerKeyFromBytes(smallKey)
	gotSess, ok := store.getByPeer(peerKey)
	if !ok {
		t.Fatal("expected session to exist in byPeer")
	}
	if gotSess.localSessionID != 100 {
		t.Errorf("expected responder session (ID=100) in byPeer, got ID=%d", gotSess.localSessionID)
	}
	if gotSess.isInitiator {
		t.Error("expected session in byPeer to be responder")
	}

	// Verify: both sessions should be accessible by localID
	if _, ok := store.getByLocalID(100); !ok {
		t.Error("responder session (ID=100) should be accessible by localID")
	}
	if _, ok := store.getByLocalID(200); !ok {
		t.Error("initiator session (ID=200) should be accessible by localID (for transition)")
	}
}

// TestSessionStore_TieBreaking_WrongRoleReplaced verifies that when an existing
// session has the wrong role, a new session with the correct role replaces it.
func TestSessionStore_TieBreaking_WrongRoleReplaced(t *testing.T) {
	smallKey := makeTestKey(0x01) // local key - smaller (should be initiator)
	largeKey := makeTestKey(0xFF) // peer key - larger

	store := newSessionStore(smallKey)

	// First: set a responder session (wrong role since local has smaller key)
	responderSess := makeTestSession(100, largeKey, false)
	store.set(responderSess)

	// Verify wrong-role session is initially in byPeer
	peerKey := types.PeerKeyFromBytes(largeKey)
	gotSess, ok := store.getByPeer(peerKey)
	if !ok {
		t.Fatal("expected session to exist in byPeer")
	}
	if gotSess.localSessionID != 100 {
		t.Errorf("expected initial responder session (ID=100), got ID=%d", gotSess.localSessionID)
	}

	// Second: set an initiator session (correct role)
	initiatorSess := makeTestSession(200, largeKey, true)
	store.set(initiatorSess)

	// Verify: initiator session should replace responder in byPeer
	gotSess, ok = store.getByPeer(peerKey)
	if !ok {
		t.Fatal("expected session to exist in byPeer after replacement")
	}
	if gotSess.localSessionID != 200 {
		t.Errorf("expected initiator session (ID=200) to replace in byPeer, got ID=%d", gotSess.localSessionID)
	}
	if !gotSess.isInitiator {
		t.Error("expected session in byPeer to be initiator after replacement")
	}

	// Verify: new session should be accessible by localID
	if _, ok := store.getByLocalID(200); !ok {
		t.Error("initiator session (ID=200) should be accessible by localID")
	}
}

// TestSessionStore_NoConflict verifies basic session storage without tie-breaking.
func TestSessionStore_NoConflict(t *testing.T) {
	localKey := makeTestKey(0x50)
	peerKey := makeTestKey(0xA0)

	store := newSessionStore(localKey)

	sess := makeTestSession(42, peerKey, true)
	store.set(sess)

	// Verify: accessible by peer key
	pk := types.PeerKeyFromBytes(peerKey)
	gotSess, ok := store.getByPeer(pk)
	if !ok {
		t.Fatal("expected session to exist in byPeer")
	}
	if gotSess.localSessionID != 42 {
		t.Errorf("expected session ID=42, got ID=%d", gotSess.localSessionID)
	}

	// Verify: accessible by local ID
	gotSess, ok = store.getByLocalID(42)
	if !ok {
		t.Fatal("expected session to exist in byLocalID")
	}
	if gotSess.localSessionID != 42 {
		t.Errorf("expected session ID=42, got ID=%d", gotSess.localSessionID)
	}
}
