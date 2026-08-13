package hazel

import (
	"os"

	"github.com/hashicorp/go-plugin"
)

// HandshakeConfig is the protocol version and magic cookie shared between
// host and plugins. Both sides must use the same values for the handshake
// to succeed.
var HandshakeConfig = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "HAZEL_PLUGIN",
	MagicCookieValue: "hazel",
}

const lifecyclePluginName = "lifecycle"

// Serve is the entry point for plugin processes. Plugin authors call this
// from main(), passing their PluginRPC implementation.
//
// Example:
//
//	func main() {
//	    hazel.Serve(&MyPlugin{})
//	}
//
// Serve handles the go-plugin handshake and RPC serving. It does not
// return; on error it logs and exits.
func Serve(impl lifecycle) {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: HandshakeConfig,
		Plugins: map[string]plugin.Plugin{
			lifecyclePluginName: &lifecyclePlugin{impl: impl},
		},
		// A non-nil set here enables gRPC; we leave it nil for pure net/rpc.
		GRPCServer: nil,
	})

	os.Exit(0)
}
