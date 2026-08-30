// Package greeter demonstrates extending hazel from outside the framework by
// registering a plugin→host service in init(). Any host or plugin binary that
// imports this package gains the "greeter" capability.
//
// Host:
//
//	import _ "github.com/yangwe11/hazel/example/greeter"
//
// Plugin (opt in to receive it):
//
//	import _ "github.com/yangwe11/hazel/example/greeter"
//
//	type MyPlugin struct{ greeter *greeter.Greeter }
//
//	func (p *MyPlugin) SetGreeter(g *greeter.Greeter) { p.greeter = g }
//
// The plugin can then call p.greeter.Greet("world") to reach the host.
//
// Registering a host→plugin service works the same way via
// hazel.RegisterPluginService, with the host consuming it via
// Manager.Dispense.
package greeter

import (
	"net/rpc"

	"github.com/yangwe11/hazel"
)

const serviceName = "greeter"

func init() {
	hazel.RegisterHostService(hazel.HostService{
		Name:   serviceName,
		Server: func(_ *hazel.Manager, _ string) any { return &greeterServer{} },
		Client: func(client *rpc.Client) any { return &Greeter{client: client} },
		Inject: func(impl, client any) {
			if ga, ok := impl.(GreeterAware); ok {
				ga.SetGreeter(client.(*Greeter))
			}
		},
	})
}

// GreeterAware lets a plugin receive the greeter during Initialize.
type GreeterAware interface {
	SetGreeter(*Greeter)
}

// Greeter is the plugin-side client for the host's greeter service.
type Greeter struct {
	client *rpc.Client
}

// Greet asks the host for a greeting for the given name.
func (g *Greeter) Greet(name string) (string, error) {
	var out string
	err := g.client.Call("Plugin.Greet", name, &out)
	return out, err
}

// greeterServer serves the host side of the greeter service.
type greeterServer struct{}

func (greeterServer) Greet(name string, out *string) error {
	*out = "hello " + name
	return nil
}
