package supervisor

import (
	"github.com/sambigeara/pollen/pkg/control"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunneling"
)

// Compile-time interface compliance checks.
var (
	_ Transport         = (*transport.QUICTransport)(nil)
	_ TransportInternal = (*transport.QUICTransport)(nil)

	_ membership.ClusterState   = (*state.Store)(nil)
	_ membership.Network        = (*transport.QUICTransport)(nil)
	_ placement.WorkloadState   = (*state.Store)(nil)
	_ tunneling.ServiceState    = (*state.Store)(nil)
	_ control.MembershipControl = (*membership.Service)(nil)
	_ control.PlacementControl  = (*placement.Service)(nil)
	_ control.TunnelingControl  = (*tunneling.Service)(nil)
	_ control.StateReader       = (*state.Store)(nil)
	_ control.TransportInfo     = (*supervisorTransportInfo)(nil)
	_ control.MetricsSource     = (*supervisorMetricsSource)(nil)
	_ control.MeshConnector     = (*Supervisor)(nil)
)
