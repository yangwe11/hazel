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
	// Config is the plugin's configuration provided by the host, JSON-encoded
	// (nil when the host supplies none). Decode it with json.Unmarshal, or read
	// the same bytes via Context.Config().
	Config []byte

	// Environment describes the host process and its environment (engine
	// version, data directory, host-defined attributes). Read it via
	// Context.Environment().
	Environment Environment

	// HostServer is the mux-broker ID of the host's hostRPC server. The host
	// sets it before calling Initialize; the plugin dials it to obtain a
	// hostRPC client. Plugins do not need to read this field directly.
	HostServer uint32

	// EventServer is the mux-broker ID of the host's built-in event bus
	// service. The plugin dials it to obtain the event bus client.
	EventServer uint32

	// HostServices maps each registered host-service name to its mux-broker
	// ID. The plugin dials each to obtain a client for that host capability.
	HostServices map[string]uint32

	// PluginID is the plugin's unique identifier in the host (its plugin.yaml
	// id). It populates Context.ID().
	PluginID string
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
	client    *rpc.Client
	broker    *plugin.MuxBroker
	host      hostRPC      // served back to the plugin during Initialize
	eventHost eventHostRPC // built-in event bus host service
	manager   *Manager     // for serving registered host services
	pluginID  string       // which plugin is being served
}

func (c *lifecycleRPC) Initialize(args InitializeArgs) error {
	// Allocate a broker ID and serve the host's hostRPC implementation so the
	// plugin can dial back to the host during its own Initialize.
	brokerID := c.broker.NextId()
	args.HostServer = brokerID
	go c.broker.AcceptAndServe(brokerID, c.host)

	// Serve the built-in event bus host service on its own connection.
	eventID := c.broker.NextId()
	args.EventServer = eventID
	go c.broker.AcceptAndServe(eventID, c.eventHost)

	// Serve each registered host service (plugin→host extension) on its own
	// broker connection and record its ID for the plugin to dial.
	registered := hostServiceSnapshot()
	args.HostServices = make(map[string]uint32, len(registered))
	for _, hs := range registered {
		id := c.broker.NextId()
		args.HostServices[hs.Name] = id
		go c.broker.AcceptAndServe(id, hs.Server(c.manager, c.pluginID))
	}

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

// Initialize dials the host's hostRPC server and event bus over the mux
// broker, assembles them into a Context, injects the Context into ContextAware
// plugins (and each registered host service into its Aware interface), then
// delegates to the implementation.
func (s *lifecycleRPCServer) Initialize(args InitializeArgs, _ *Empty) error {
	conn, err := s.broker.Dial(args.HostServer)
	if err != nil {
		return err
	}

	host := &hostFacade{hostClient: &hostRPCClient{client: rpc.NewClient(conn)}}

	// Dial the event bus host service and bind it to the shared plugin bus.
	var bus EventBus
	if s.eventBus != nil {
		eventConn, err := s.broker.Dial(args.EventServer)
		if err != nil {
			return err
		}
		s.eventBus.eventHost = &eventHostRPCClient{client: rpc.NewClient(eventConn)}
		bus = s.eventBus
	}

	// Inject the assembled Context into ContextAware plugins.
	if ca, ok := s.impl.(ContextAware); ok {
		ca.SetContext(&pluginContext{
			id:     args.PluginID,
			host:   host,
			bus:    bus,
			config: args.Config,
			env:    args.Environment,
		})
	}

	// Dial and inject registered host services (plugin→host extensions).
	for _, hs := range hostServiceSnapshot() {
		id, ok := args.HostServices[hs.Name]
		if !ok {
			continue
		}
		conn, err := s.broker.Dial(id)
		if err != nil {
			return err
		}
		client := hs.Client(rpc.NewClient(conn))
		if hs.Inject != nil {
			hs.Inject(s.impl, client)
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
	impl      Lifecycle       // plugin side: the implementation to serve
	host      hostRPC         // host side: what to expose to the plugin
	eventHost eventHostRPC    // host side: built-in event bus host service
	manager   *Manager        // host side: for serving registered host services
	eventBus  *pluginEventBus // plugin side: shared with the event plugin
	pluginID  string          // host side: which plugin is being served
}

// Server runs on the PLUGIN side.
func (p *lifecyclePlugin) Server(broker *plugin.MuxBroker) (interface{}, error) {
	return &lifecycleRPCServer{impl: p.impl, broker: broker, eventBus: p.eventBus}, nil
}

// Client runs on the HOST side.
func (p *lifecyclePlugin) Client(broker *plugin.MuxBroker, client *rpc.Client) (interface{}, error) {
	return &lifecycleRPC{client: client, broker: broker, host: p.host, eventHost: p.eventHost, manager: p.manager, pluginID: p.pluginID}, nil
}
