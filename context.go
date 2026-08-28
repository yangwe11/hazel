package hazel

// Context is the single session handle the host injects into a plugin. The
// plugin receives it during Initialize by implementing ContextAware.
//
// A Context is plugin-scoped and lives for one plugin lifecycle run; a
// restarted plugin receives a fresh Context.
//
// Context carries the core, always-present capabilities (identity, host, event
// bus). Optional extension capabilities (registered via RegisterHostService)
// are still delivered through their own Aware interfaces.
type Context interface {
	// ID is the plugin's unique identifier in the host (its plugin.yaml id).
	ID() string

	// Host exposes host-side capabilities served by the host process (today
	// just Ping; health, query, and admin surface grow here).
	Host() Host

	// Bus is the host's event bus: publish events and subscribe to topic
	// patterns. Equivalent to the former standalone EventBus.
	Bus() EventBus
}

// ContextAware is the optional interface a plugin implements to receive its
// Context during Initialize.
type ContextAware interface {
	SetContext(Context)
}

type pluginContext struct {
	id   string
	host Host
	bus  EventBus
}

func (c *pluginContext) ID() string {
	return c.id
}

func (c *pluginContext) Host() Host {
	return c.host
}

func (c *pluginContext) Bus() EventBus {
	return c.bus
}
