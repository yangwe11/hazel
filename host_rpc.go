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

// HostRPC defines the methods a plugin can call on the host. Each method must
// follow net/rpc's signature: Method(args T, reply *U) error.
//
// To add a callback, add a method here and mirror it on HostRPCClient (below)
// and hostRPCAdapter (in manager.go).
type HostRPC interface {
	// Ping lets a plugin verify its connection to the host is alive.
	Ping(Empty, *Empty) error
}

// HostRPCClient implements HostRPC in the plugin process, translating each
// call into an rpc.Call over the broker connection.
type HostRPCClient struct {
	client *rpc.Client
}

func (c *HostRPCClient) Ping(_ Empty, _ *Empty) error {
	return c.client.Call("Plugin.Ping", Empty{}, &Empty{})
}

// hostRPCAdapter exposes the Manager to plugins as HostRPC. It is separate from
// Manager so the RPC surface stays minimal and explicit — adding an exported
// method to Manager does not accidentally expose it to plugins.
type hostRPCAdapter struct {
	manager *Manager
}

func (a *hostRPCAdapter) Ping(_ Empty, _ *Empty) error {
	// TODO: report real liveness once health tracking is added.
	return nil
}
