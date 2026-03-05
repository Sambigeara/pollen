package nat

import (
	"net/netip"
	"sync"
)

// Type classifies a node's NAT behavior.
type Type int

const (
	Unknown Type = iota
	Easy         // endpoint-independent: same external port for all destinations
	Hard         // endpoint-dependent: port varies by destination (symmetric)
)

func (t Type) String() string {
	switch t {
	case Easy:
		return "easy"
	case Hard:
		return "hard"
	default:
		return "unknown"
	}
}

// ToUint32 converts a Type to the wire representation used in gossip.
func (t Type) ToUint32() uint32 {
	return uint32(t)
}

// TypeFromUint32 converts a wire representation back to a Type.
func TypeFromUint32(v uint32) Type {
	switch Type(v) { //nolint:mnd
	case Easy:
		return Easy
	case Hard:
		return Hard
	default:
		return Unknown
	}
}

// Detector determines the local NAT type by comparing ObservedAddress
// reports from peers with distinct public IPs. Same external port across
// observers → Easy. Different ports → Hard.
type Detector struct {
	observations map[netip.Addr]int
	natType      Type
	mu           sync.RWMutex
}

func NewDetector() *Detector {
	return &Detector{
		observations: make(map[netip.Addr]int),
	}
}

// AddObservation records that the observer at observerIP saw us on the given
// port. Returns the (possibly updated) NAT type and whether it changed.
func (d *Detector) AddObservation(observerIP netip.Addr, port int) (Type, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.observations[observerIP]; !exists && len(d.observations) >= maxObservations {
		clear(d.observations)
	}
	d.observations[observerIP] = port

	prev := d.natType
	d.natType = d.evaluate()
	return d.natType, d.natType != prev
}

// Type returns the current NAT type determination.
func (d *Detector) Type() Type {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.natType
}

const (
	minObservations = 2
	maxObservations = 16
)

func (d *Detector) evaluate() Type {
	if len(d.observations) < minObservations {
		return Unknown
	}

	var firstPort int
	first := true
	for _, port := range d.observations {
		if first {
			firstPort = port
			first = false
			continue
		}
		if port != firstPort {
			return Hard
		}
	}
	return Easy
}
