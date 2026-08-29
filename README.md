# hazel

> [简体中文](README.zh-CN.md)

Hazel is a Go plugin framework built on [hashicorp/go-plugin]. It runs your
application as a **host** process plus a set of **plugin** processes, connected
over `net/rpc`, with lifecycle management, an event bus, configuration delivery,
an extension registry, and log collection built in.

[hashicorp/go-plugin]: https://github.com/hashicorp/go-plugin

## Features

- **Lifecycle** — discover, load, initialize, start, and stop plugins, with
  dependency-ordered parallel startup and crash monitoring.
- **Event bus** — the host and plugins publish and subscribe to dot-separated
  topic patterns (`plugin.running`, `config.changed`, …).
- **Configuration** — per-plugin JSON config pushed at Initialize, plus runtime
  updates delivered as `config.changed` events.
- **Environment** — shared host facts (engine version, data directory, custom
  attributes) delivered to every plugin.
- **Extensions** — add host↔plugin capabilities from outside the framework with
  a two-line registry.
- **Log collection** — plugin stderr/stdout is merged into the host's `hclog`
  logger, prefixed with the plugin name.

## Install

```
go get github.com/yangwe11/hazel
```

## Quick start

### The plugin

Every plugin implements `hazel.Lifecycle` and is served by `hazel.Serve`. It
declares its metadata in a `plugin.yaml` file next to its binary.

```go
package main

import "github.com/yangwe11/hazel"

type Greeter struct{ ctx hazel.Context }

// SetContext is optional; implement it to receive the Context at Initialize.
func (g *Greeter) SetContext(ctx hazel.Context) { g.ctx = ctx }

func (g *Greeter) Initialize(hazel.InitializeArgs) error { return nil }
func (g *Greeter) Start(hazel.StartArgs) error            { return nil }
func (g *Greeter) Stop() error                            { return nil }

func main() {
    hazel.Serve(&Greeter{})
}
```

### The host

```go
package main

import "github.com/yangwe11/hazel"

func main() {
    m, err := hazel.NewManager(hazel.ManagerConfig{
        PluginDirs: []string{"./plugins"},
    })
    if err != nil {
        panic(err)
    }
    defer m.Shutdown()

    if _, err := m.Discover(); err != nil {
        panic(err)
    }
    for _, id := range []string{"greeter"} {
        m.Load(id)
        m.Initialize(id)
        m.Start(id)
    }
    // plugins are now running
}
```

## Concepts

### Lifecycle & state

A plugin moves through `unloaded → loaded → initialized → running`, and can end
in `stopped` or `error`. The host drives this with `Manager.Load`,
`Manager.Initialize`, `Manager.Start`, and `Manager.Stop`. `StartArgs.First` is
true on the first successful start of a plugin in a host session.

### Context

A plugin that implements `hazel.ContextAware` receives a `hazel.Context` during
`Initialize`. It is the plugin's core handle:

```go
ctx.ID()            // the plugin's id
ctx.Host().Ping()   // reach the host
ctx.Bus()           // the event bus
ctx.Config()        // per-plugin config, JSON-encoded
ctx.Environment()   // shared host facts
```

### Event bus

Publish and subscribe to dot-separated topic patterns. `*` matches one segment,
`>` matches the rest.

```go
ctx.Bus().Subscribe("config.changed", func(e hazel.Event) {
    // handle the event
})
ctx.Bus().Publish(hazel.Event{Name: "app.ready", Payload: "started"})
```

### Configuration

The host supplies config per plugin; the plugin decodes it:

```go
// host
cfg := hazel.DefaultManagerConfig()
cfg.Config = func(id string) (any, error) {
    return map[string]any{"port": 8080}, nil
}

// plugin
var c struct{ Port int `json:"port"` }
json.Unmarshal(ctx.Config(), &c)
```

Push a runtime update — plugins receive a `config.changed` event:

```go
m.UpdateConfig("greeter", map[string]any{"port": 9090})
```

### Environment

Shared, host-defined facts reach every plugin via `Context.Environment()`:

```go
env := ctx.Environment()
env.EngineVersion  // the running engine version
env.DataDir        // ManagerConfig.DataDir
env.Attributes     // host-defined map[string]string
```

### Extensions

Add new capabilities without touching the framework. Register in `init()`, then
import the package from both binaries.

- **plugin → host**: `hazel.RegisterHostService` — the host serves it, plugins
  consume it via an `*Aware` interface. See
  [`example/greeter`](example/greeter/greeter.go).
- **host → plugin**: `hazel.RegisterPluginService` — the plugin serves it, the
  host consumes it via `Manager.Dispense`.

### Logging

Plugin logs are collected automatically: go-plugin reads each plugin's stderr
line-by-line and re-emits it through the host's logger, prefixed with the plugin
name. Configure it via `ManagerConfig.Logger` (an `hclog.Logger`); the default
writes to stderr at `Info` level. Note that un-prefixed `log.Printf` output is
classified as `Debug` by go-plugin.

### Discovery

`Manager.Discover` scans each `PluginDirs` entry one level deep for
`plugin.yaml`:

```yaml
id: greeter
name: Greeter
version: 1.0.0
type: NATIVE      # NATIVE | STATIC | BRIDGE
cmdName: greeter  # optional, defaults to id
engineRequirement: ">=0.1.0"  # optional
depends: []       # optional, with version constraints
```

## License

[Apache 2.0](LICENSE) — Copyright (c) 2026 yangwe11.
