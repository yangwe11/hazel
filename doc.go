// Package hazel is a plugin framework for Go, built on hashicorp/go-plugin.
//
// hazel splits an application into a host process and a set of plugin
// processes. The host discovers plugins on disk, drives their lifecycle
// (load → initialize → start → stop), and mediates communication between
// them; each plugin runs as a separate OS process and talks to the host over
// go-plugin's net/rpc transport.
//
// # Two roles
//
// A plugin binary calls [Serve] from main with a value that implements
// [Lifecycle]:
//
//	func main() {
//	    hazel.Serve(&Greeter{})
//	}
//
// The host creates a [Manager], discovers plugins, and drives their lifecycle:
//
//	m, err := hazel.NewManager(hazel.ManagerConfig{PluginDirs: []string{"./plugins"}})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer m.Shutdown()
//
//	m.Discover()
//	m.Load("greeter")
//	m.Initialize("greeter")
//	m.Start("greeter")
//
// # Built-in capabilities
//
//   - Lifecycle — every plugin implements [Lifecycle]; the host drives it and
//     tracks each plugin's state.
//   - [Context] — the single handle a plugin receives at Initialize, carrying
//     identity, host access, the event bus, configuration, and environment.
//   - Event bus — a hub-and-spoke router; the host and plugins publish and
//     subscribe to dot-separated topic patterns.
//   - Configuration — the host pushes per-plugin JSON config at Initialize and
//     may update it at runtime via [Manager.UpdateConfig].
//   - Environment — shared host facts (engine version, data directory, and
//     custom attributes) delivered to every plugin.
//   - Extensions — [RegisterHostService] (plugin→host) and
//     [RegisterPluginService] (host→plugin) add capabilities from outside the
//     framework.
//
// See the repository README for a fuller walkthrough and examples.
package hazel
