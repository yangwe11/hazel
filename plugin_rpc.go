package hazel

import (
	"net/rpc"

	"github.com/hashicorp/go-plugin"
)

// =========================================================================
// Host → Plugin RPC (lifecycle)
//
// Plugin authors implement Lifecycle. The host drives it over go-plugin's
// net/rpc transport.
// =========================================================================

// Lifecycle is the interface every plugin must implement. The host calls these
// methods to manage the plugin's lifecycle.
type Lifecycle interface {
	// Initialize runs once after the plugin process starts and before Start.
	// Use it to load configuration and prepare resources.
	Initialize(args InitializeArgs) error

	// Start begins the plugin's work.
	Start(args StartArgs) error

	// Stop asks the plugin to shut down gracefully.
	Stop() error
}

// InitializeArgs carries initialization data from the host to the plugin.
type InitializeArgs struct {
	// Config holds plugin-specific configuration provided by the host.
	Config map[string]any

	// HostServer is the mux-broker ID of the host's HostRPC server. The host
	// sets it before calling Initialize; the plugin dials it to obtain a
	// HostRPC client. Plugins do not need to read this field directly.
	HostServer uint32
}

// StartArgs carries parameters for Start.
type StartArgs struct {
	// First is true on the plugin's first successful start in this host
	// session, and false on every restart that follows a Stop or crash.
	First bool
}

// Empty is a placeholder used for RPC methods with no arguments or result.
type Empty struct{}

// =========================================================================
// Host-side RPC client (the host drives the plugin's Lifecycle)
// =========================================================================

// lifecycleRPC implements Lifecycle in the host process by translating each
// method into an rpc.Call on the plugin's connection.
type lifecycleRPC struct {
	client  *rpc.Client
	broker  *plugin.MuxBroker
	hostRPC HostRPC // served back to the plugin during Initialize
}

func (c *lifecycleRPC) Initialize(args InitializeArgs) error {
	// Allocate a broker ID and serve the host's HostRPC implementation so the
	// plugin can dial back to the host during its own Initialize.
	brokerID := c.broker.NextId()
	args.HostServer = brokerID
	go c.broker.AcceptAndServe(brokerID, c.hostRPC)

	reply := Empty{}
	return c.client.Call("Plugin.Initialize", args, &reply)
}

func (c *lifecycleRPC) Start(args StartArgs) error {
	reply := Empty{}
	return c.client.Call("Plugin.Start", args, &reply)
}

func (c *lifecycleRPC) Stop() error {
	reply := Empty{}
	return c.client.Call("Plugin.Stop", Empty{}, &reply)
}

// =========================================================================
// Plugin-side RPC server
// =========================================================================

// lifecycleRPCServer wraps a Lifecycle implementation and serves it over
// net/rpc in the plugin process.
type lifecycleRPCServer struct {
	impl   Lifecycle
	broker *plugin.MuxBroker
}

// Initialize dials the host's HostRPC server over the mux broker, injects it
// into HostAware plugins, then delegates to the implementation.
func (s *lifecycleRPCServer) Initialize(args InitializeArgs, _ *Empty) error {
	conn, err := s.broker.Dial(args.HostServer)
	if err != nil {
		return err
	}

	hostClient := &HostRPCClient{client: rpc.NewClient(conn)}
	if ha, ok := s.impl.(HostAware); ok {
		ha.SetHostRPC(hostClient)
	}

	return s.impl.Initialize(args)
}

func (s *lifecycleRPCServer) Start(args StartArgs, _ *Empty) error {
	return s.impl.Start(args)
}

func (s *lifecycleRPCServer) Stop(_ *Empty, _ *Empty) error {
	return s.impl.Stop()
}

// lifecyclePlugin implements plugin.Plugin, bridging Lifecycle over net/rpc
// between the host and plugin processes.
type lifecyclePlugin struct {
	impl    Lifecycle // plugin side: the implementation to serve
	hostRPC HostRPC   // host side: what to expose to the plugin
}

// Server runs on the PLUGIN side.
func (p *lifecyclePlugin) Server(broker *plugin.MuxBroker) (interface{}, error) {
	return &lifecycleRPCServer{impl: p.impl, broker: broker}, nil
}

// Client runs on the HOST side.
func (p *lifecyclePlugin) Client(broker *plugin.MuxBroker, client *rpc.Client) (interface{}, error) {
	return &lifecycleRPC{client: client, broker: broker, hostRPC: p.hostRPC}, nil
}
