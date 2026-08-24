package hazel

import "net/rpc"

// =========================================================================
// Plugin → Host RPC (core host)
//
// The host exposes a hostRPC server to every plugin over go-plugin's mux
// broker (no separate TCP listener). The wire-level hostRPC surface is hidden
// behind the concise Host interface; a plugin opts in by implementing
// HostAware to receive a Host during Initialize.
//
// The event bus is a separate built-in host service with its own broker
// connection (see event.go); it is not part of hostRPC.
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

// hostRPC is the wire-level RPC surface for the core host capability. Each
// method must follow net/rpc's signature: Method(args T, reply *U) error.
//
// To add a host capability: add the clean method to Host, the wire method
// here, then mirror it on hostRPCServer, hostRPCClient, and hostFacade.
type hostRPC interface {
	// Ping lets a plugin verify its connection to the host is alive.
	Ping(Empty, *Empty) error
}

// hostRPCClient implements hostRPC in the plugin process, translating each
// call into an rpc.Call over the broker connection.
type hostRPCClient struct {
	client *rpc.Client
}

func (c *hostRPCClient) Ping(_ Empty, _ *Empty) error {
	return c.client.Call("Plugin.Ping", Empty{}, &Empty{})
}

// hostRPCServer serves the core host capability on the host side. It is
// separate from Manager so the RPC surface stays minimal and explicit — adding
// an exported method to Manager does not accidentally expose it to plugins.
type hostRPCServer struct{}

func (hostRPCServer) Ping(_ Empty, _ *Empty) error {
	// TODO: report real liveness once health tracking is added.
	return nil
}

// hostFacade exposes the clean, plugin-facing Host API on top of a hostRPCClient.
// It is the plugin-side implementation of Host, hiding the wire-level RPC
// signature (Ping(Empty, *Empty) error) behind a concise facade.
type hostFacade struct {
	hostClient *hostRPCClient // wire-level client to the host
}

func (f *hostFacade) Ping() error {
	return f.hostClient.Ping(Empty{}, &Empty{})
}
