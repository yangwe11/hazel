package hazel

import (
	"github.com/hashicorp/go-plugin"
)

// EngineVersion is the hazel engine version. Plugins declare an engineRequirement
// against this value; Discover report plugins whose requirement is not met.
// Bump this on every release following semantic versioning.
const EngineVersion = "0.1.0"

// HandshakeConfig is the protocol version and magic cookie shared between the
// host and plugins. Both sides must use identical values for the handshake to
// succeed.
var HandshakeConfig = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "HAZEL_PLUGIN",
	MagicCookieValue: "hazel",
}

const (
	lifecyclePluginName = "lifecycle"
	eventPluginName     = "event"
)

// Serve is the plugin entry point. Plugin authors call it from main(), passing
// a type that implements Lifecycle:
//
//	func main() {
//	    hazel.Serve(&MyPlugin{})
//	}
//
// Serve performs the go-plugin handshake and blocks serving RPC until the host
// disconnects. It never returns.
func Serve(impl Lifecycle) {
	// One shared event bus serves both the lifecycle plugin (which injects it
	// via the Context Bus) and the event plugin (which receives events).
	bus := &pluginEventBus{handlers: make(map[string]pluginHandler)}

	plugins := map[string]plugin.Plugin{
		lifecyclePluginName: &lifecyclePlugin{impl: impl, eventBus: bus},
		eventPluginName:     &eventPlugin{bus: bus},
	}
	// Merge registered plugin services (host→plugin extensions).
	for name, p := range pluginServiceSnapshot() {
		plugins[name] = p
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: HandshakeConfig,
		Plugins:         plugins,
		// GRPCServer is nil, so hazel uses go-plugin's default net/rpc transport.
	})
}
