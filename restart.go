package hazel

import "time"

// RestartPolicy configures automatic restart of crashed plugins.
type RestartPolicy struct {
	// MaxRetries is the maximum number of consecutive automatic restarts in a
	// crash loop. Once exhausted, the plugin stays in StateError until the host
	// restarts it manually (which resets the counter). Zero disables
	// auto-restart.
	MaxRetries int

	// Backoff is the delay before each restart attempt. Zero restarts
	// immediately.
	Backoff time.Duration
}

// maybeAutoRestart restarts a crashed plugin if auto-restart is configured and
// the restart budget is not exhausted. It is called by the crash monitor just
// before it exits; the restart itself runs in a fresh goroutine after the
// backoff, because Load waits for the monitor to exit.
func (m *Manager) maybeAutoRestart(pi *PluginInstance) {
	p := m.config.Restart
	if p == nil || p.MaxRetries <= 0 {
		return
	}

	pi.stateMu.Lock()
	if pi.restarts >= p.MaxRetries {
		pi.stateMu.Unlock()
		m.log.Printf("plugin %s: giving up after %d auto-restart(s)", pi.Meta.ID, p.MaxRetries)
		return
	}
	pi.restarts++
	attempt := pi.restarts
	pi.autoRestarting = true
	pi.stateMu.Unlock()

	pluginID := pi.Meta.ID
	go func() {
		if p.Backoff > 0 {
			time.Sleep(p.Backoff)
		}
		m.log.Printf("plugin %s: auto-restarting (attempt %d/%d)", pluginID, attempt, p.MaxRetries)
		if err := m.restartOne(pluginID); err != nil {
			m.log.Printf("plugin %s: auto-restart failed: %v", pluginID, err)
		}
	}()
}

// restartOne re-runs the Load→Initialize→Start sequence for a plugin.
func (m *Manager) restartOne(pluginID string) error {
	if err := m.Load(pluginID); err != nil {
		return err
	}
	if err := m.Initialize(pluginID); err != nil {
		return err
	}
	return m.Start(pluginID)
}
