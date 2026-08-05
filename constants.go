package hazel

const (
	// PluginTypeStatic identifies a static plugin that is not launched by the engine.
	PluginTypeStatic = "STATIC"
	// PluginTypeNative identifies a native plugin. Go is currently supported.
	PluginTypeNative = "NATIVE"
	// PluginTypeBridge identifies a bridge plugin that wraps an external service (e.g., Redis, Kafka).
	PluginTypeBridge = "BRIDGE"
)