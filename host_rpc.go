package hazel

import "net/rpc"

// =========================================================================
// Plugin → Host RPC
//
// The host exposes a HostRPC server to every plugin over go-plugin's mux
// broker (no separate TCP listener). A plugin opts in by implementing
// HostAware; the host injects a HostRPC client during Initialize.
// =========================================================================

// HostAware is an optional interface a plugin may implement to receive a
// HostRPC client during Initialize.
type HostAware interface {
	SetHostRPC(HostRPC)
}

// EventAware is an optional interface a plugin implements to receive an
// EventBus during Initialize.
type EventAware interface {
	SetEventBus(EventBus)
}

// HostRPC defines the methods a plugin can call on the host. Each method must
// follow net/rpc's signature: Method(args T, reply *U) error.
//
// To add a callback, add a method here and mirror it on HostRPCClient (below)
// and hostRPCAdapter (in manager.go).
type HostRPC interface {
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

// HostRPCClient implements HostRPC in the plugin process, translating each
// call into an rpc.Call over the broker connection.
type HostRPCClient struct {
	client *rpc.Client
}

func (c *HostRPCClient) Ping(_ Empty, _ *Empty) error {
	return c.client.Call("Plugin.Ping", Empty{}, &Empty{})
}

func (c *HostRPCClient) Publish(event Event, _ *Empty) error {
	return c.client.Call("Plugin.Publish", event, &Empty{})
}

func (c *HostRPCClient) Subscribe(pattern string, reply *string) error {
	return c.client.Call("Plugin.Subscribe", pattern, reply)
}

func (c *HostRPCClient) Unsubscribe(id string, _ *Empty) error {
	return c.client.Call("Plugin.Unsubscribe", id, &Empty{})
}

// hostRPCAdapter exposes the Manager to plugins as HostRPC. It is separate from
// Manager so the RPC surface stays minimal and explicit — adding an exported
// method to Manager does not accidentally expose it to plugins.
type hostRPCAdapter struct {
	manager  *Manager
	pluginID string // which plugin this adapter serves
}

func (a *hostRPCAdapter) Ping(_ Empty, _ *Empty) error {
	// TODO: report real liveness once health tracking is added.
	return nil
}

func (a *hostRPCAdapter) Publish(event Event, _ *Empty) error {
	// Stamp the publisher so subscribers always know who produced the event.
	if event.Source == "" {
		event.Source = a.pluginID
	}
	a.manager.events.publish(event)
	return nil
}

func (a *hostRPCAdapter) Subscribe(pattern string, reply *string) error {
	id, err := a.manager.events.subscribe(a.pluginID, pattern, nil)
	if err != nil {
		return err
	}
	*reply = id
	return nil
}

func (a *hostRPCAdapter) Unsubscribe(id string, _ *Empty) error {
	return a.manager.events.unsubscribe(a.pluginID, id)
}
