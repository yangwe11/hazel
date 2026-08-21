package hazel

import (
	"fmt"
	"net/rpc"
	"sync"

	"github.com/hashicorp/go-plugin"
)

// =========================================================================
// Extension registry
//
// hazel's core capabilities (lifecycle, event delivery, hostRPC) are wired
// directly in Serve/Load/Initialize. Everything here is the public mechanism
// for ADDING capabilities from outside hazel: host and plugin authors write a
// package that registers itself in init(), then import it from both binaries.
//
// Registration is intended to happen in init(), before Serve/NewManager. The
// registry is guarded by a mutex and each registration rejects duplicate names
// (including hazel's built-in names), so a stray import cannot silently
// replace a core capability.
// =========================================================================

// RegisterPluginService registers a host→plugin capability: the plugin serves
// it, and the host consumes it via Manager.Dispense. p is a go-plugin plugin.
//
// Register before Serve (plugin side) or NewManager (host side); init() is the
// natural place. Panics if name is empty, already registered, or collides with
// a built-in service name.
func RegisterPluginService(name string, p plugin.Plugin) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if name == "" {
		panic("hazel: plugin service name must not be empty")
	}
	if name == lifecyclePluginName || name == eventPluginName {
		panic(fmt.Sprintf("hazel: plugin service %q conflicts with a built-in service", name))
	}
	if _, ok := pluginServices[name]; ok {
		panic(fmt.Sprintf("hazel: plugin service %q already registered", name))
	}
	pluginServices[name] = p
}

// HostService describes a plugin→host capability: the host serves it, and
// plugins consume it.
type HostService struct {
	// Name is the unique service name, also used as the broker key.
	Name string

	// Server builds the host-side net/rpc receiver (methods of the form
	// Method(args, *reply) error).
	Server func(m *Manager) any

	// Client builds the plugin-side wire client from the dialed RPC connection.
	Client func(client *rpc.Client) any

	// Inject hands the Client result to a plugin that opts in via an *Aware
	// interface. May be nil for services consumed another way.
	Inject func(impl, client any)
}

// RegisterHostService registers a plugin→host capability. Register before
// Serve/NewManager; init() is the natural place. Panics if the service name is
// empty or already registered.
func RegisterHostService(s HostService) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if s.Name == "" {
		panic("hazel: host service name must not be empty")
	}
	if _, ok := hostServices[s.Name]; ok {
		panic(fmt.Sprintf("hazel: host service %q already registered", s.Name))
	}
	hostServices[s.Name] = s
}

// Registries, populated by the registration functions above.
var (
	registryMu     sync.Mutex
	pluginServices = map[string]plugin.Plugin{}
	hostServices   = map[string]HostService{}
)

// pluginServiceSnapshot returns a copy of the registered plugin services,
// safe to iterate while concurrent registrations may occur.
func pluginServiceSnapshot() map[string]plugin.Plugin {
	registryMu.Lock()
	defer registryMu.Unlock()

	out := make(map[string]plugin.Plugin, len(pluginServices))
	for name, p := range pluginServices {
		out[name] = p
	}
	return out
}

// hostServiceSnapshot returns a copy of the registered host services, safe to
// iterate while concurrent registrations may occur.
func hostServiceSnapshot() []HostService {
	registryMu.Lock()
	defer registryMu.Unlock()

	out := make([]HostService, 0, len(hostServices))
	for _, s := range hostServices {
		out = append(out, s)
	}
	return out
}
