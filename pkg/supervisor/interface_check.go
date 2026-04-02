package supervisor

import (
	"github.com/sambigeara/pollen/pkg/control"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunneling"
	"github.com/sambigeara/pollen/pkg/wasm"
)

// Compile-time interface compliance checks.
var (
	_ Transport         = (*transport.QUICTransport)(nil)
	_ TransportInternal = (*transport.QUICTransport)(nil)

	_ membership.ClusterState   = state.StateStore(nil)
	_ membership.Network        = (*transport.QUICTransport)(nil)
	_ placement.WorkloadState   = state.StateStore(nil)
	_ tunneling.ServiceState    = state.StateStore(nil)
	_ control.MembershipControl = (*membership.Service)(nil)
	_ control.PlacementControl  = (*placement.Service)(nil)
	_ control.TunnelingControl  = (*tunneling.Service)(nil)
	_ control.StateReader       = state.StateStore(nil)
	_ control.TransportInfo     = (*supervisorTransportInfo)(nil)
	_ control.MetricsSource     = (*supervisorMetricsSource)(nil)
	_ control.MeshConnector     = (*Supervisor)(nil)

	_ placement.WASMRuntime = (*wasm.Runtime)(nil)
)
