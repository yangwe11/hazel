package hazel

import "net/rpc"

// HostAware is an optional interface that plugins can implement to receive
// a HostRPC client during initialization. If a plugin implements HostAware,
// the PluginRPCServer calls SetHostRPC after connecting to the host.
type HostAware interface {
	SetHostRPC(HostRPC)
}

// =========================================================================
// Plugin → Host RPC interface
//
// The host implements this interface and exposes it to plugins via a TCP
// listener. Plugins call these methods to interact with the host.
// =========================================================================

// HostRPC defines the methods a plugin can call on the host.
type HostRPC interface {
}

// =========================================================================
// Plugin-side RPC client (plugin uses this to call the host)
// =========================================================================

// HostRPCClient wraps a raw *rpc.Client and implements HostRPC by
// translating each method into an rpc.Call over a TCP connection to the
// host. It lives in the plugin process.
type HostRPCClient struct {
	client *rpc.Client
}

// =========================================================================
// Host-side RPC server (serves HostRPC to plugins over TCP)
// =========================================================================

// HostRPCServer wraps a HostRPC implementation and serves it via net/rpc.
// It is the object registered with rpc.RegisterName and exposed to plugins
// over a TCP listener.
type HostRPCServer struct {
	delegate HostRPC
}
