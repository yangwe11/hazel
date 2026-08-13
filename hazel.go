package hazel

import (
	"github.com/hashicorp/go-plugin"
)

// HandshakeConfig is the protocol version and magic cookie shared between the
// host and plugins. Both sides must use identical values for the handshake to
// succeed.
var HandshakeConfig = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "HAZEL_PLUGIN",
	MagicCookieValue: "hazel",
}

const lifecyclePluginName = "lifecycle"

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
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: HandshakeConfig,
		Plugins: map[string]plugin.Plugin{
			lifecyclePluginName: &lifecyclePlugin{impl: impl},
		},
		// GRPCServer is nil, so hazel uses go-plugin's default net/rpc transport.
	})
}
