package hazel

// =========================================================================
// Host → Plugin RPC interface
//
// Plugins must implement this interface. The host calls these methods
// over net/rpc via go-plugin to manage the plugin's lifecycle.
// =========================================================================

// PluginRPC defines the lifecycle methods the host calls on a plugin.
type PluginRPC interface {
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


