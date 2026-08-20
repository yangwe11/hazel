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

	// HostServer is the mux-broker ID of the host's hostRPC server. The host
	// sets it before calling Initialize; the plugin dials it to obtain a
	// hostRPC client. Plugins do not need to read this field directly.
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
	client *rpc.Client
	broker *plugin.MuxBroker
	host   hostRPC // served back to the plugin during Initialize
}

func (c *lifecycleRPC) Initialize(args InitializeArgs) error {
	// Allocate a broker ID and serve the host's hostRPC implementation so the
	// plugin can dial back to the host during its own Initialize.
	brokerID := c.broker.NextId()
	args.HostServer = brokerID
	go c.broker.AcceptAndServe(brokerID, c.host)

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
	impl     Lifecycle
	broker   *plugin.MuxBroker
	eventBus *pluginEventBus
}

// Initialize dials the host's hostRPC server over the mux broker, injects it
// into HostAware plugins, gives EventAware plugins the event bus, then
// delegates to the implementation.
func (s *lifecycleRPCServer) Initialize(args InitializeArgs, _ *Empty) error {
	conn, err := s.broker.Dial(args.HostServer)
	if err != nil {
		return err
	}

	hostClient := &hostRPCClient{client: rpc.NewClient(conn)}
	if ha, ok := s.impl.(HostAware); ok {
		ha.SetHost(&hostFacade{hostClient: hostClient})
	}
	if s.eventBus != nil {
		s.eventBus.host = hostClient
		if ea, ok := s.impl.(EventAware); ok {
			ea.SetEventBus(s.eventBus)
		}
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
	impl     Lifecycle       // plugin side: the implementation to serve
	host     hostRPC         // host side: what to expose to the plugin
	eventBus *pluginEventBus // plugin side: shared with the event plugin
}

// Server runs on the PLUGIN side.
func (p *lifecyclePlugin) Server(broker *plugin.MuxBroker) (interface{}, error) {
	return &lifecycleRPCServer{impl: p.impl, broker: broker, eventBus: p.eventBus}, nil
}

// Client runs on the HOST side.
func (p *lifecyclePlugin) Client(broker *plugin.MuxBroker, client *rpc.Client) (interface{}, error) {
	return &lifecycleRPC{client: client, broker: broker, host: p.host}, nil
}

// =========================================================================
// Host → Plugin RPC (event delivery)
//
// The host delivers events to a plugin by calling Deliver on the plugin's
// event server over the broker connection.
// =========================================================================

// eventRPC delivers events from the host to a plugin's event server over the
// broker connection.
type eventRPC struct {
	client *rpc.Client
}

// Deliver sends one event to the plugin's event server.
func (c *eventRPC) Deliver(event Event) error {
	return c.client.Call("Plugin.Deliver", event, &Empty{})
}

// eventRPCServer receives events from the host over net/rpc and dispatches
// them to the plugin's local handlers.
type eventRPCServer struct {
	bus *pluginEventBus
}

// Deliver is called by the host to push one event into the plugin process.
func (s *eventRPCServer) Deliver(event Event, _ *Empty) error {
	s.bus.dispatch(event)
	return nil
}

// eventPlugin bridges event delivery over net/rpc between host and plugin.
type eventPlugin struct {
	bus *pluginEventBus // plugin side: where delivered events are dispatched
}

// Server runs on the PLUGIN side.
func (p *eventPlugin) Server(_ *plugin.MuxBroker) (interface{}, error) {
	return &eventRPCServer{bus: p.bus}, nil
}

// Client runs on the HOST side.
func (p *eventPlugin) Client(_ *plugin.MuxBroker, client *rpc.Client) (interface{}, error) {
	return &eventRPC{client: client}, nil
}
