package nat

import (
	"net/netip"
	"sync"
)

type Type int

const (
	Unknown Type = iota
	Easy         // endpoint-independent: same external port for all destinations
	Hard         // endpoint-dependent: port varies by destination (symmetric)

	minObservationsHard = 2
	minObservationsEasy = 3
	maxObservations     = 16
)

func (t Type) ToUint32() uint32 {
	return uint32(t)
}

func TypeFromUint32(v uint32) Type {
	switch Type(v) {
	case Easy:
		return Easy
	case Hard:
		return Hard
	default:
		return Unknown
	}
}

type observation struct {
	ip   netip.Addr
	port int
}

// Detector determines the local NAT type by comparing ObservedAddress
// reports from peers with distinct public IPs. Same external port across
// observers → Easy. Different ports → Hard.
type Detector struct {
	observations []observation
	natType      Type
	mu           sync.RWMutex
}

func NewDetector() *Detector {
	return &Detector{
		observations: make([]observation, 0, maxObservations),
	}
}

// AddObservation records that the observer at observerIP saw us on the given
// port. Returns the (possibly updated) NAT type and whether it changed.
func (d *Detector) AddObservation(observerIP netip.Addr, port int) (Type, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, obs := range d.observations {
		if obs.ip == observerIP {
			d.observations = append(d.observations[:i], d.observations[i+1:]...)
			break
		}
	}
	if len(d.observations) == maxObservations {
		d.observations = d.observations[1:]
	}
	d.observations = append(d.observations, observation{ip: observerIP, port: port})

	prev := d.natType
	d.natType = d.evaluate()
	return d.natType, d.natType != prev
}

func (d *Detector) Type() Type {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.natType
}

func (d *Detector) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.observations = d.observations[:0]
	d.natType = Unknown
}

func (d *Detector) evaluate() Type {
	if len(d.observations) < minObservationsHard {
		return Unknown
	}

	var firstPort int
	first := true
	for _, obs := range d.observations {
		if first {
			firstPort = obs.port
			first = false
			continue
		}
		if obs.port != firstPort {
			return Hard
		}
	}
	if len(d.observations) < minObservationsEasy {
		return Unknown
	}
	return Easy
}
