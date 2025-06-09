package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/conc/pool"
)

type Node struct {
	log   logrus.FieldLogger
	addr  string
	set   *store.LWWSet
	peers []string
	mu    sync.RWMutex
	pool  *pool.ContextPool
}

func NewNode(log logrus.FieldLogger, addr, peers string) *Node {
	n := &Node{
		log:   log,
		addr:  addr,
		set:   store.NewLWWSet(),
		peers: []string{},
	}

	if peers != "" {
		for peer := range strings.SplitSeq(peers, ",") {
			n.addPeer(peer)
		}
	}

	return n
}

func (n *Node) Start(ctx context.Context) error {
	go n.startGossip(ctx)
	go n.listen(ctx)

	<-ctx.Done()
	return ctx.Err()
}

func (n *Node) startGossip(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	n.log.Info("Starting gossiping...")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n.mu.RLock()
			peers := n.peers
			n.mu.RUnlock()

			for _, peer := range peers {
				go func() {
					if err := n.gossipToPeer(peer); err != nil {
						n.log.Errorf("Failed to gossip to peer %s: %v", peer, err)
					}
				}()
			}
		}
	}
}

func (n *Node) addPeer(peer string) {
	n.mu.Lock()
	n.peers = append(n.peers, peer)
	n.mu.Unlock()
}

// TODO(saml) needs to be dynamically managed
func (n *Node) listen(ctx context.Context) error {
	ln, err := net.Listen("tcp", n.addr)
	if err != nil {
		n.log.Errorf("Unable to listen to address: %s", n.addr)
		return err
	}
	defer ln.Close()
	n.log.Infof("Node listening on %s", n.addr)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				n.log.Errorf("Accept error: %v", err)
				continue
			}
		}

		go n.handleConnection(conn)
	}
}

func (n *Node) handleConnection(conn net.Conn) error {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	var peerSet store.LWWSet
	if err := decoder.Decode(&peerSet); err != nil {
		n.log.Errorf("Error decoding peer set: %v", err)
		return fmt.Errorf("Decode error: %v", err)
	}

	n.mu.Lock()
	n.set.Merge(&peerSet)
	n.mu.Unlock()

	n.log.Infof("Merged state from peer, now have %d functions", len(n.set.Elements))

	return nil
}

func (n *Node) gossipToPeer(peer string) error {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		return fmt.Errorf("Failed to connect to %s: %v", peer, err)
	}
	defer conn.Close()

	n.mu.RLock()
	defer n.mu.RUnlock()

	if err := json.NewEncoder(conn).Encode(n.set); err != nil {
		return fmt.Errorf("Failed to send to %s: %v", peer, err)
	}

	return nil
}

func (n *Node) addFunction(id, hash string) {
	n.mu.Lock()
	n.set.Add(id, hash)
	n.mu.Unlock()
	n.log.Infof("Added function %s with hash %s", id, hash)
}
