package hazel

import (
	"net/rpc"

	"github.com/hashicorp/go-plugin"
)

// pluginRunner implements go-plugin's plugin.Plugin interface for net/rpc
// transport between host and plugin processes.
type pluginRunner struct {
	impl PluginRPC
}

// Server is called on the PLUGIN SIDE. It returns an RPC server object that
// go-plugin registers and serves over the plugin's stdin/stdout connection.
func (r *pluginRunner) Server(broker *plugin.MuxBroker) (interface{}, error) {
	return &PluginRPCServer{
		impl:   r.impl,
		broker: broker,
	}, nil
}

// Client is called on the HOST SIDE. It receives the raw *rpc.Client and
// returns a typed PluginRPC stub the host uses to drive the plugin.
func (r *pluginRunner) Client(broker *plugin.MuxBroker, client *rpc.Client) (interface{}, error) {
	return &PluginRPCClient{
		client: client,
		broker: broker,
	}, nil
}

// =========================================================================
// Plugin-side RPC server
// =========================================================================

// PluginRPCServer wraps a PluginRPC implementation and serves it via net/rpc.
// It lives in the plugin process and is registered by go-plugin.
type PluginRPCServer struct {
	impl   PluginRPC
	broker *plugin.MuxBroker
}

// Initialize connects to the host's TCP-based HostRPC server to establish
// bidirectional communication, then delegates to the plugin implementation.
func (s *PluginRPCServer) Initialize(args InitializeArgs) error {
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

func (s *PluginRPCServer) Start(args StartArgs) error {
	return s.impl.Start(args)
}

func (s *PluginRPCServer) Stop() error {
	return s.impl.Stop()
}

// =========================================================================
// Host-side RPC client
// =========================================================================

// PluginRPCClient wraps a raw *rpc.Client and implements PluginRPC by
// translating each method call into an rpc.Call. It lives in the host
// process.
type PluginRPCClient struct {
	client  *rpc.Client
	broker  *plugin.MuxBroker
	hostRPC HostRPC
}

func (c *PluginRPCClient) Initialize(args InitializeArgs) error {
	brokerID := c.broker.NextId()
	args.HostServer = brokerID
	go c.broker.AcceptAndServe(brokerID, c.hostRPC)
	reply := struct{}{}
	return c.client.Call("Plugin.Initialize", args, &reply)
}

func (c *PluginRPCClient) Start(args StartArgs) error {
	reply := struct{}{}
	return c.client.Call("Plugin.Start", args, &reply)
}

func (c *PluginRPCClient) Stop() error {
	return c.client.Call("Plugin.Stop", nil, nil)
}
