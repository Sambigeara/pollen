package node

import (
	"encoding/json"
	"log"
	"net"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/store"
)

type Node struct {
	addr  string
	set   *store.LWWSet
	peers []string
	mu    sync.RWMutex
}

func New(addr string) *Node {
	return &Node{
		addr:  addr,
		set:   store.NewLWWSet(),
		peers: []string{},
	}
}

func (n *Node) AddPeer(peer string) {
	n.mu.Lock()
	n.peers = append(n.peers, peer)
	n.mu.Unlock()
}

func (n *Node) Listen() {
	ln, err := net.Listen("tcp", n.addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Node listening on %s", n.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go n.handleConnection(conn)
	}
}

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	var peerSet store.LWWSet
	if err := decoder.Decode(&peerSet); err != nil {
		log.Printf("Decode error: %v", err)
		return
	}

	n.mu.Lock()
	n.set.Merge(&peerSet)
	n.mu.Unlock()

	log.Printf("Merged state from peer, now have %d functions", len(n.set.Elements))
}

func (n *Node) StartGossip() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		n.mu.RLock()
		peers := n.peers
		n.mu.RUnlock()

		for _, peer := range peers {
			go n.gossipToPeer(peer)
		}
	}
}

func (n *Node) gossipToPeer(peer string) {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Printf("Failed to connect to %s: %v", peer, err)
		return
	}
	defer conn.Close()

	n.mu.RLock()
	defer n.mu.RUnlock()

	if err := json.NewEncoder(conn).Encode(n.set); err != nil {
		log.Printf("Failed to send to %s: %v", peer, err)
	}
}

func (n *Node) AddFunction(id, hash string) {
	n.mu.Lock()
	n.set.Add(id, hash)
	n.mu.Unlock()
	log.Printf("Added function %s with hash %s", id, hash)
}
