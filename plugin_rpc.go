package hazel

import (
	"net/rpc"

	"github.com/hashicorp/go-plugin"
)

// =========================================================================
// Host → Plugin RPC interface
//
// Plugins must implement this interface. The host calls these methods
// over net/rpc via go-plugin to manage the plugin's lifecycle.
// =========================================================================

// lifecycle defines the lifecycle methods the host calls on a plugin.
type lifecycle interface {
	// Initialize the plugin will connect the HostRPC over the connect by go-plugin
	Initialize(args InitializeArgs) error
	Start(args StartArgs) error
	Stop() error
}

// InitializeArgs carries the configuration and environment info the host
// passes to a plugin during initialization.
type InitializeArgs struct {
	// Config holds plugin-specific configuration from the host.
	Config map[string]any

	// HostServer ID
	HostServer uint32
}

// StartArgs carries parameters for starting plugin work.
// First indicate the first start.
type StartArgs struct {
	First bool
}

type Empty struct{}

// =========================================================================
// Host-side RPC client
// =========================================================================

// lifecycleRPC wraps a raw *rpc.Client and implements lifecycle by
// translating each method call into an rpc.Call. It lives in the host
// process.
type lifecycleRPC struct {
	client  *rpc.Client
	broker  *plugin.MuxBroker
	hostRPC HostRPC
}

func (c *lifecycleRPC) Initialize(args InitializeArgs) error {
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

// lifecycleRPCServer wraps a lifecycle implementation and serves it via net/rpc.
// It lives in the plugin process and is registered by go-plugin.
type lifecycleRPCServer struct {
	impl   lifecycle
	broker *plugin.MuxBroker
}

// Initialize connects to the host's TCP-based HostRPC server to establish
// bidirectional communication, then delegates to the plugin implementation.
func (s *lifecycleRPCServer) Initialize(args InitializeArgs, _ *Empty) error {
	conn, err := s.broker.Dial(args.HostServer)
	if err != nil {
		return err
	}

	hostClient := &HostRPCClient{client: rpc.NewClient(conn)}

	// If the plugin implements HostAware, inject the HostRPC client so the
	// plugin can call back to the host.
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

// lifecyclePlugin implements go-plugin's plugin.Plugin interface for net/rpc
// transport between host and plugin processes.
type lifecyclePlugin struct {
	impl lifecycle
}

// Server is called on the PLUGIN SIDE. It returns an RPC server object that
// go-plugin registers and serves over the plugin's connection.
func (r *lifecyclePlugin) Server(broker *plugin.MuxBroker) (interface{}, error) {
	return &lifecycleRPCServer{
		impl:   r.impl,
		broker: broker,
	}, nil
}

// Client is called on the HOST SIDE. It receives the raw *rpc.Client and
// returns a typed lifecycle stub the host uses to drive the plugin.
func (r *lifecyclePlugin) Client(broker *plugin.MuxBroker, client *rpc.Client) (interface{}, error) {
	return &lifecycleRPC{
		client: client,
		broker: broker,
	}, nil
}
