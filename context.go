package hazel

// Environment describes the host process and its environment. The host fills it
// in and passes it to every plugin during Initialize; a plugin reads it via
// Context.Environment().
//
// Environment is host→plugin, read-only data, distinct from Config: Config is
// per-plugin and JSON-shaped (arbitrary nesting), while Environment is shared
// scalar facts every plugin sees identically.
type Environment struct {
	// EngineVersion is the running engine's version (hazel.EngineVersion).
	// It is passed at runtime rather than read from the plugin's own compile,
	// so a plugin can detect when it was built against a different engine.
	EngineVersion string

	// DataDir is the manager's data directory, where plugins may persist
	// state. Empty when ManagerConfig.DataDir is unset.
	DataDir string

	// Attributes holds host-defined, shared scalar facts (for example
	// "environment=staging"). Keys and values are strings; richer or nested
	// data belongs in the plugin's Config instead.
	Attributes map[string]string
}

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

	// Config is the plugin's configuration provided by the host, JSON-encoded
	// (nil when none). Decode it with json.Unmarshal.
	Config() []byte

	// Environment describes the host process and its environment (engine
	// version, data directory, and host-defined attributes).
	Environment() Environment
}

// ContextAware is the optional interface a plugin implements to receive its
// Context during Initialize.
type ContextAware interface {
	SetContext(Context)
}

type pluginContext struct {
	id     string
	host   Host
	bus    EventBus
	config []byte
	env    Environment
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

func (c *pluginContext) Config() []byte {
	return c.config
}

func (c *pluginContext) Environment() Environment {
	return c.env
}
