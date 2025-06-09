package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/conc/pool"
)

type Node struct {
	log  logrus.FieldLogger
	addr string
	set  *store.NodeStore
	mu   sync.RWMutex
	pool *pool.ContextPool
}

func NewNode(log logrus.FieldLogger, addr string, peers []string) *Node {
	n := &Node{
		log:  log,
		addr: addr,
		set:  store.NewNodeStore(log, addr, peers),
	}

	return n
}

func (n *Node) Start(ctx context.Context) error {
	go n.listen(ctx)
	go n.publishPeers(ctx)

	<-ctx.Done()
	return ctx.Err()
}

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

		go n.consumePeers(conn)
	}
}

func (n *Node) consumePeers(conn net.Conn) error {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	var peerStore store.NodeStore
	if err := decoder.Decode(&peerStore); err != nil {
		n.log.Errorf("Error decoding peer set: %v", err)
		return fmt.Errorf("Decode error: %v", err)
	}

	n.mu.Lock()
	n.set.Merge(&peerStore)
	n.mu.Unlock()

	n.log.Debugf("Merged state from peer %s, now have %d functions", peerStore.OriginAddr, len(n.set.Functions))

	return nil
}

func (n *Node) publishPeers(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n.mu.RLock()
			peers := n.GetPeerAddresses()
			n.mu.RUnlock()

			for _, peer := range peers {
				go func() {
					if err := n.gossipToPeer(peer); err != nil {
						n.log.Debugf("Failed to publish to peer %s: %v", peer, err)
						n.set.DisablePeer(peer)
					}
				}()
			}
		}
	}
}

// TODO(saml) phase out arbitrary JSON
func (n *Node) gossipToPeer(peer string) error {
	n.log.Debugf("publishing to peer: %s", peer)

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
	n.set.AddFunction(id, hash)
	n.mu.Unlock()
	n.log.Infof("Added function %s with hash %s", id, hash)
}

func (s *Node) GetPeerAddresses() []string {
	peers := s.set.GetPeers()
	addrs := make([]string, 0, len(peers))
	for _, p := range peers {
		if p.Addr == s.addr {
			continue
		}
		addrs = append(addrs, p.Addr)
	}

	return addrs
}
