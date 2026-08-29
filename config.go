package hazel

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
)

// =========================================================================
// Configuration
//
// Configuration flows from the host to plugins in two directions, each backed
// by a different primitive:
//
//   - Static: the host hands each plugin its config at Initialize via
//     ManagerConfig.Config; the plugin reads it from Context.Config() (or the
//     InitializeArgs it receives). Config crosses the wire as JSON bytes so it
//     needs no gob registration, whatever its shape.
//
//   - Dynamic: the host replaces a plugin's config at runtime with
//     Manager.UpdateConfig, which publishes a ConfigChangedTopic event. Plugins
//     subscribe via their event bus and react to the change.
// =========================================================================

// ConfigChangedTopic is the event topic the host publishes when a plugin's
// configuration is replaced at runtime via Manager.UpdateConfig.
const ConfigChangedTopic = "config.changed"

// ConfigChanged is the payload of a ConfigChangedTopic event. Config holds the
// new configuration, JSON-encoded; decode it with json.Unmarshal.
type ConfigChanged struct {
	PluginID string // which plugin's configuration changed
	Config   []byte // the new configuration, JSON-encoded
}

// init registers ConfigChanged so runtime config updates flow between the host
// and plugins without manual gob.Register calls.
func init() {
	gob.Register(ConfigChanged{})
}

// UpdateConfig replaces a plugin's configuration at runtime and notifies
// subscribers by publishing a ConfigChangedTopic event. The plugin's
// Context.Config() still reflects the original Initialize-time configuration;
// runtime updates arrive only through the event.
//
// cfg must be JSON-marshalable. The plugin must be registered (its ID known to
// the manager); it need not be Running.
func (m *Manager) UpdateConfig(pluginID string, cfg any) error {
	if _, err := m.getPlugin(pluginID); err != nil {
		return err
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encode config for plugin %s: %w", pluginID, err)
	}

	m.events.publish(Event{
		Name:    ConfigChangedTopic,
		Source:  pluginID,
		Payload: ConfigChanged{PluginID: pluginID, Config: data},
	})
	return nil
}
