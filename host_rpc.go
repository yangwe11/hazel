package hazel

import "net/rpc"

// =========================================================================
// Plugin → Host RPC
//
// The host exposes a hostRPC server to every plugin over go-plugin's mux
// broker (no separate TCP listener). The wire-level hostRPC surface is hidden
// behind the concise Host interface; a plugin opts in by implementing
// HostAware to receive a Host during Initialize.
// =========================================================================

// Host is the concise host API exposed to plugins. New capabilities grow this
// interface. Plugins opt in via HostAware.
type Host interface {
	Ping() error
}

// HostAware is an optional interface a plugin may implement to receive a
// Host during Initialize.
type HostAware interface {
	SetHost(Host)
}

// hostRPC is the wire-level RPC surface the host serves. Each method must
// follow net/rpc's signature: Method(args T, reply *U) error.
//
// To add a host capability: add the clean method to Host, the wire method
// here, then mirror it on hostRPCServer, hostRPCClient, and hostFacade.
type hostRPC interface {
	// Ping lets a plugin verify its connection to the host is alive.
	Ping(Empty, *Empty) error

	// Publish sends an event to every subscriber.
	Publish(Event, *Empty) error

	// Subscribe registers this plugin's interest in a topic pattern and
	// returns a subscription ID.
	Subscribe(pattern string, reply *string) error

	// Unsubscribe removes a subscription previously created by this plugin.
	Unsubscribe(id string, _ *Empty) error
}

// hostRPCClient implements hostRPC in the plugin process, translating each
// call into an rpc.Call over the broker connection.
type hostRPCClient struct {
	client *rpc.Client
}

func (c *hostRPCClient) Ping(_ Empty, _ *Empty) error {
	return c.client.Call("Plugin.Ping", Empty{}, &Empty{})
}

func (c *hostRPCClient) Publish(event Event, _ *Empty) error {
	return c.client.Call("Plugin.Publish", event, &Empty{})
}

func (c *hostRPCClient) Subscribe(pattern string, reply *string) error {
	return c.client.Call("Plugin.Subscribe", pattern, reply)
}

func (c *hostRPCClient) Unsubscribe(id string, _ *Empty) error {
	return c.client.Call("Plugin.Unsubscribe", id, &Empty{})
}

// hostRPCServer exposes the Manager to plugins as hostRPC. It is separate from
// Manager so the RPC surface stays minimal and explicit — adding an exported
// method to Manager does not accidentally expose it to plugins.
type hostRPCServer struct {
	manager  *Manager
	pluginID string // which plugin is connected
}

func (s *hostRPCServer) Ping(_ Empty, _ *Empty) error {
	// TODO: report real liveness once health tracking is added.
	return nil
}

func (s *hostRPCServer) Publish(event Event, _ *Empty) error {
	// Stamp the publisher so subscribers always know who produced the event.
	if event.Source == "" {
		event.Source = s.pluginID
	}
	s.manager.events.publish(event)
	return nil
}

func (s *hostRPCServer) Subscribe(pattern string, reply *string) error {
	id, err := s.manager.events.subscribe(s.pluginID, pattern, nil)
	if err != nil {
		return err
	}
	*reply = id
	return nil
}

func (s *hostRPCServer) Unsubscribe(id string, _ *Empty) error {
	return s.manager.events.unsubscribe(s.pluginID, id)
}

// hostFacade exposes the clean, plugin-facing Host API on top of a hostRPCClient.
// It is the plugin-side implementation of Host, hiding the wire-level RPC
// signatures (Ping(Empty, *Empty) error) behind a concise facade.
type hostFacade struct {
	hostClient *hostRPCClient // wire-level client to the host
}

func (f *hostFacade) Ping() error {
	return f.hostClient.Ping(Empty{}, &Empty{})
}
