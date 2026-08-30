package hazel

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
)

// ManagerConfig holds the configuration for a plugin Manager.
type ManagerConfig struct {
	// PluginDirs is the list of directories to scan for plugins.
	PluginDirs []string

	// DataDir is the path where plugins may persist state.
	DataDir string

	// MaxParallel is the maximum number of plugins to start concurrently
	// within a dependency batch. Zero means no limit.
	MaxParallel int

	// StartTimeout is the per-plugin deadline for the Load→Initialize→Start
	// sequence.
	StartTimeout time.Duration

	// StopTimeout is the per-plugin deadline for graceful shutdown.
	StopTimeout time.Duration

	// Command customizes how the plugin process is launched, e.g. to inject
	// extra flags or environment variables. If nil, the binary at execPath is
	// executed directly.
	Command func(execPath string) *exec.Cmd

	// Config returns the JSON-marshalable configuration for a plugin, keyed by
	// plugin ID. It is called during Initialize; return (nil, nil) for plugins
	// with no configuration. If Config is nil, no plugin receives configuration.
	Config func(pluginID string) (any, error)

	// Attributes holds host-defined scalar facts (environment, region, etc.)
	// shared with every plugin via Context.Environment(). Keys and values are strings.
	Attributes map[string]string

	// Logger is the hclog logger the host uses for go-plugin internals and for
	// collecting plugin logs. go-plugin reads each plugin's stderr line-by-line
	// and re-emits it through this logger with the plugin binary's name as a
	// prefix; the plugin's os.Stdout/os.Stderr are also synced to the host's
	// stdout/stderr. If nil, a logger named "hazel" writing to stderr at Info
	// level is used.
	Logger hclog.Logger

	// Restart, if set, configures automatic restart of crashed plugins. If nil
	// (the default), a crashed plugin stays in StateError until restarted
	// manually.
	Restart *RestartPolicy
}

// DefaultManagerConfig returns a ManagerConfig with sensible defaults.
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		MaxParallel:  0,
		StartTimeout: 30 * time.Second,
		StopTimeout:  10 * time.Second,
	}
}

// PluginInstance wraps a plugin's metadata and runtime state managed by
// the host.
type PluginInstance struct {
	Meta      PluginMeta
	PluginDir string
	State     PluginState

	// go-plugin internals (set during Load)
	client          *plugin.Client
	lifecycleClient Lifecycle
	eventClient     *eventRPC // host-side event delivery client (set during Initialize)

	// started is true once Start has succeeded at least once; it drives
	// StartArgs.First across restarts.
	started bool

	// restarts counts consecutive auto-restarts; autoRestarting is true while an
	// auto-restart sequence is in flight (see RestartPolicy).
	restarts       int
	autoRestarting bool

	stateMu   sync.Mutex
	changedAt time.Time
	listeners []chan StateChange
	// onTransition, when set, is invoked after every state change. The manager
	// uses it to publish lifecycle events and clean up event subscriptions.
	onTransition func(StateChange)

	// stopCh is closed to tell the crash monitor a shutdown is expected. It is
	// recreated on each Start and closed by Stop.
	stopCh   chan struct{}
	stopOnce sync.Once
	// monitorWG tracks the crash monitor goroutine so Stop and Load can wait
	// for it to exit before resetting per-lifecycle state.
	monitorWG sync.WaitGroup
}

// TransitionTo attempts to move the plugin to the given state. It returns
// an error if the transition is not allowed.
func (pi *PluginInstance) TransitionTo(target PluginState, cause error) error {
	pi.stateMu.Lock()
	defer pi.stateMu.Unlock()

	if !CanTransition(pi.State, target) {
		return fmt.Errorf("%w: cannot transition from %s to %s",
			ErrInvalidStateTransition, pi.State, target)
	}

	change := StateChange{
		PluginID: pi.Meta.ID,
		From:     pi.State,
		To:       target,
		Err:      cause,
		Time:     time.Now(),
	}

	pi.State = target
	pi.changedAt = change.Time

	for _, ch := range pi.listeners {
		select {
		case ch <- change:
		default:
		}
	}
	if pi.onTransition != nil {
		pi.onTransition(change)
	}
	return nil
}

// Listen returns a channel that receives state changes for this plugin.
// The channel has a buffer size of 8; slow receivers may miss events.
func (pi *PluginInstance) Listen() <-chan StateChange {
	pi.stateMu.Lock()
	defer pi.stateMu.Unlock()

	ch := make(chan StateChange, 8)
	pi.listeners = append(pi.listeners, ch)
	return ch
}

// signalStop marks the plugin's process as intentionally stopping. It is safe
// to call more than once.
func (pi *PluginInstance) signalStop() {
	pi.stopOnce.Do(func() { close(pi.stopCh) })
}

// Manager manages the plugin lifecycle: discovery, loading, initialization,
// start, stop, and dependency-ordered parallel startup.
type Manager struct {
	config  ManagerConfig
	mu      sync.RWMutex
	plugins map[string]*PluginInstance

	events *eventBus // routes events between plugins and the host

	log        *log.Logger
	shutdownCh chan struct{}
}

// NewManager creates a Manager. Plugins are registered via Discover before
// their lifecycle is driven.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if cfg.Logger == nil {
		cfg.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "hazel",
			Level:  hclog.Info,
			Output: os.Stderr,
		})
	}

	m := &Manager{
		config:     cfg,
		plugins:    make(map[string]*PluginInstance),
		log:        log.New(os.Stdout, "[hazel] ", log.LstdFlags),
		shutdownCh: make(chan struct{}),
	}
	m.events = newEventBus(m.log)
	return m, nil
}

// Events returns the manager's event bus, which host code uses to publish
// events and subscribe to plugin and application events.
func (m *Manager) Events() EventBus {
	return m.events
}

// =========================================================================
// Discovery
// =========================================================================

// Discover scans all configured plugin directories and adds discovered
// plugins to the registry in the Unloaded state. Plugins that are already
// registered (by ID) are skipped.
//
// Discover registers nothing and returns an error if any plugin ID appears
// more than once in the scan, or if any plugin's engineRequirement is not
// satisfied by the running engine version. Returns the number of newly
// discovered plugins.
func (m *Manager) Discover() (int, error) {
	var all []PluginMeta
	for _, dir := range m.config.PluginDirs {
		metas, err := ScanDirectory(dir)
		if err != nil {
			return 0, fmt.Errorf("scan %s: %w", dir, err)
		}
		all = append(all, metas...)
	}

	// Reject duplicate IDs so the registry keeps a unique ID per plugin.
	var duplicates []error
	seen := make(map[string]string, len(all)) // ID → plugin directory
	for _, meta := range all {
		if prev, ok := seen[meta.ID]; ok {
			duplicates = append(duplicates,
				fmt.Errorf("%w: plugin ID %q already discovered in %s (now in %s)",
					ErrDuplicatePluginID, meta.ID, prev, meta.pluginDir))
			continue
		}
		seen[meta.ID] = meta.pluginDir
	}
	if len(duplicates) > 0 {
		return 0, errors.Join(duplicates...)
	}

	// Reject the whole discovery up front if any plugin is incompatible with
	// the engine, so the caller can fix the metadata and retry cleanly.
	var incompatible []error
	for _, meta := range all {
		if err := checkEngineCompatibility(meta); err != nil {
			incompatible = append(incompatible, err)
		}
	}
	if len(incompatible) > 0 {
		return 0, errors.Join(incompatible...)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for _, meta := range all {
		if _, exists := m.plugins[meta.ID]; exists {
			continue
		}
		m.plugins[meta.ID] = &PluginInstance{
			Meta:         meta,
			PluginDir:    meta.pluginDir,
			State:        StateUnloaded,
			stopCh:       make(chan struct{}),
			onTransition: m.onPluginTransition,
		}
		count++
	}
	return count, nil
}

// =========================================================================
// Per-plugin lifecycle
// =========================================================================

// Load validates the plugin binary and creates the go-plugin client for it.
// State: Unloaded → Loaded.
func (m *Manager) Load(pluginID string) error {
	pi, err := m.getPlugin(pluginID)
	if err != nil {
		return err
	}

	if !CanTransition(pi.State, StateLoaded) {
		return fmt.Errorf("%w: cannot load from %s", ErrInvalidStateTransition, pi.State)
	}

	// Wait for any prior crash monitor to exit before resetting per-lifecycle
	// state, so a restart never races a stale monitor or client.
	pi.monitorWG.Wait()

	// Clean up the previous lifecycle's client, if any.
	if pi.client != nil {
		pi.client.Kill()
		pi.client = nil
	}
	pi.lifecycleClient = nil
	pi.eventClient = nil

	cmdName := pi.Meta.CmdName
	if cmdName == "" {
		cmdName = pi.Meta.ID
	}
	execPath := filepath.Join(pi.PluginDir, cmdName)

	var cmd *exec.Cmd
	if m.config.Command != nil {
		cmd = m.config.Command(execPath)
	} else {
		cmd = exec.Command(execPath)
	}

	plugins := map[string]plugin.Plugin{
		lifecyclePluginName: &lifecyclePlugin{host: &hostRPCServer{}, eventHost: &eventHostRPCServer{manager: m, pluginID: pluginID}, manager: m, pluginID: pluginID},
		eventPluginName:     &eventPlugin{},
	}
	// Merge registered plugin services (host→plugin extensions).
	for name, p := range pluginServiceSnapshot() {
		plugins[name] = p
	}

	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: HandshakeConfig,
		Plugins:         plugins,
		Cmd:             cmd,
		Managed:         true,
		SyncStdout:      os.Stdout,
		SyncStderr:      os.Stderr,
		Logger:          m.config.Logger,
	})

	pi.client = client

	if err := pi.TransitionTo(StateLoaded, nil); err != nil {
		return err
	}

	m.log.Printf("plugin %s loaded (binary: %s)", pluginID, execPath)
	return nil
}

// Initialize starts the plugin process and calls the Initialize RPC.
// State: Loaded → Initialized.
func (m *Manager) Initialize(pluginID string) error {
	pi, err := m.getPlugin(pluginID)
	if err != nil {
		return err
	}

	if !CanTransition(pi.State, StateInitialized) {
		return fmt.Errorf("%w: cannot initialize from %s", ErrInvalidStateTransition, pi.State)
	}

	// Start the plugin process (handshake). Start returns the address
	// the plugin is listening on, but we don't need it for net/rpc.
	if _, err := pi.client.Start(); err != nil {
		pi.TransitionTo(StateError, fmt.Errorf("start process: %w", err))
		return fmt.Errorf("start plugin %s: %w", pluginID, err)
	}

	// Dispense the RPC client.
	protocol, err := pi.client.Client()
	if err != nil {
		pi.client.Kill()
		pi.TransitionTo(StateError, fmt.Errorf("dispense rpc: %w", err))
		return fmt.Errorf("connect to plugin %s: %w", pluginID, err)
	}
	dispensed, err := protocol.Dispense(lifecyclePluginName)
	if err != nil {
		pi.client.Kill()
		pi.TransitionTo(StateError, fmt.Errorf("dispense: %w", err))
		return fmt.Errorf("dispense plugin %s: %w", pluginID, err)
	}
	pi.lifecycleClient = dispensed.(Lifecycle)

	// Dispense the event delivery client and register it with the event bus
	// before Initialize, since the plugin may subscribe during Initialize.
	eventDispensed, err := protocol.Dispense(eventPluginName)
	if err != nil {
		pi.client.Kill()
		pi.TransitionTo(StateError, fmt.Errorf("dispense event: %w", err))
		return fmt.Errorf("dispense event plugin %s: %w", pluginID, err)
	}
	pi.eventClient = eventDispensed.(*eventRPC)
	m.events.registerPlugin(pluginID, pi.eventClient.Deliver)

	// Initialize the plugin, passing host configuration and the host's RPC
	// endpoints so the plugin can call back to the host.
	args := InitializeArgs{
		PluginID: pluginID,
		Environment: Environment{
			EngineVersion: EngineVersion,
			DataDir:       m.config.DataDir,
			Attributes:    m.config.Attributes,
		},
	}
	if m.config.Config != nil {
		cfg, err := m.config.Config(pluginID)
		if err != nil {
			pi.client.Kill()
			pi.TransitionTo(StateError, fmt.Errorf("load config: %w", err))
			return fmt.Errorf("load config for plugin %s: %w", pluginID, err)
		}
		if cfg != nil {
			data, err := json.Marshal(cfg)
			if err != nil {
				pi.client.Kill()
				pi.TransitionTo(StateError, fmt.Errorf("encode config: %w", err))
				return fmt.Errorf("encode config for plugin %s: %w", pluginID, err)
			}
			args.Config = data
		}
	}
	if err := pi.lifecycleClient.Initialize(args); err != nil {
		pi.client.Kill()
		pi.TransitionTo(StateError, fmt.Errorf("initialize rpc: %w", err))
		return fmt.Errorf("initialize plugin %s: %w", pluginID, err)
	}

	// Wire each registered host service so a bidirectional capability can bind
	// its host→plugin direction now that the plugin is initialized.
	for _, hs := range hostServiceSnapshot() {
		if hs.Wire == nil {
			continue
		}
		if err := hs.Wire(m, pluginID); err != nil {
			pi.client.Kill()
			pi.TransitionTo(StateError, fmt.Errorf("wire %s: %w", hs.Name, err))
			return fmt.Errorf("wire %s for plugin %s: %w", hs.Name, pluginID, err)
		}
	}

	if err := pi.TransitionTo(StateInitialized, nil); err != nil {
		return err
	}

	m.log.Printf("plugin %s initialized", pluginID)
	return nil
}

// Start calls the Start RPC on an initialized plugin.
// State: Initialized → Running.
func (m *Manager) Start(pluginID string) error {
	pi, err := m.getPlugin(pluginID)
	if err != nil {
		return err
	}

	if !CanTransition(pi.State, StateRunning) {
		return fmt.Errorf("%w: cannot start from %s", ErrInvalidStateTransition, pi.State)
	}

	first := !pi.started
	if err := pi.lifecycleClient.Start(StartArgs{First: first}); err != nil {
		pi.TransitionTo(StateError, fmt.Errorf("start rpc: %w", err))
		return fmt.Errorf("start plugin %s: %w", pluginID, err)
	}
	pi.started = true

	// Reset the auto-restart counter on a manual start; an auto-restart leaves
	// the counter in place so the crash-loop budget is enforced.
	pi.stateMu.Lock()
	if !pi.autoRestarting {
		pi.restarts = 0
	}
	pi.autoRestarting = false
	pi.stateMu.Unlock()

	// Recreate the monitor signal for this run and start crash monitoring.
	pi.stopCh = make(chan struct{})
	pi.stopOnce = sync.Once{}
	pi.monitorWG.Add(1)
	go m.monitor(pi)

	if err := pi.TransitionTo(StateRunning, nil); err != nil {
		return err
	}

	m.log.Printf("plugin %s started", pluginID)
	return nil
}

// Stop calls the Stop RPC and terminates the plugin process.
// State: Running|Initialized → Stopped.
func (m *Manager) Stop(pluginID string) error {
	pi, err := m.getPlugin(pluginID)
	if err != nil {
		return err
	}

	if !CanTransition(pi.State, StateStopped) {
		return fmt.Errorf("%w: cannot stop from %s", ErrInvalidStateTransition, pi.State)
	}

	// Tell the crash monitor this exit is expected and wait for it to finish so
	// it cannot report the termination as a crash or race a later restart.
	pi.signalStop()
	pi.monitorWG.Wait()

	// Attempt graceful stop via RPC if the plugin is initialized or running.
	if pi.lifecycleClient != nil && (pi.State == StateInitialized || pi.State == StateRunning) {
		done := make(chan error, 1)
		go func() {
			done <- pi.lifecycleClient.Stop()
		}()

		select {
		case err := <-done:
			if err != nil {
				m.log.Printf("plugin %s: graceful stop error: %v", pluginID, err)
			}
		case <-time.After(m.config.StopTimeout):
			m.log.Printf("plugin %s: stop timeout, force-killing", pluginID)
		}
	}

	// Ensure the process is terminated.
	if pi.client != nil {
		pi.client.Kill()
	}

	if err := pi.TransitionTo(StateStopped, nil); err != nil {
		return err
	}

	m.log.Printf("plugin %s stopped", pluginID)
	return nil
}

// =========================================================================
// Registry queries
// =========================================================================

// getPlugin returns the PluginInstance for the given ID.
func (m *Manager) getPlugin(id string) (*PluginInstance, error) {
	m.mu.RLock()
	pi, ok := m.plugins[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPluginNotFound, id)
	}
	return pi, nil
}

// GetPlugin returns the PluginInstance for the given ID, or nil if not found.
func (m *Manager) GetPlugin(id string) *PluginInstance {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pi, _ := m.plugins[id]
	return pi
}

// ListPlugins returns all registered plugin instances, regardless of state.
func (m *Manager) ListPlugins() []*PluginInstance {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*PluginInstance, 0, len(m.plugins))
	for _, pi := range m.plugins {
		result = append(result, pi)
	}
	return result
}

// Dispense returns a client for a registered plugin service on the given
// plugin. The plugin must have been Initialized first. The returned value is
// the result of the service's plugin.Plugin.Client method; the caller asserts
// the concrete type.
func (m *Manager) Dispense(pluginID, serviceName string) (any, error) {
	pi, err := m.getPlugin(pluginID)
	if err != nil {
		return nil, err
	}
	if pi.client == nil {
		return nil, fmt.Errorf("plugin %s is not loaded", pluginID)
	}

	protocol, err := pi.client.Client()
	if err != nil {
		return nil, fmt.Errorf("connect to plugin %s: %w", pluginID, err)
	}
	return protocol.Dispense(serviceName)
}

// =========================================================================
// Internal helpers
// =========================================================================

// onPluginTransition reacts to a plugin's state change by publishing a
// "plugin.<state>" event and, on terminal states, dropping the plugin's event
// subscriptions and delivery worker.
func (m *Manager) onPluginTransition(change StateChange) {
	le := LifecycleEvent{
		PluginID: change.PluginID,
		From:     change.From.String(),
		To:       change.To.String(),
	}
	if change.Err != nil {
		le.Err = change.Err.Error()
	}

	m.events.publish(Event{
		Name:    "plugin." + change.To.String(),
		Source:  change.PluginID,
		Payload: le,
		Time:    change.Time,
	})

	if change.To == StateStopped || change.To == StateError {
		m.events.unregisterPlugin(change.PluginID)
	}
}

// monitor polls a running plugin for unexpected exit. It exits when the
// plugin is stopped intentionally (stopCh) or the manager shuts down.
func (m *Manager) monitor(pi *PluginInstance) {
	defer pi.monitorWG.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if pi.client.Exited() {
				m.log.Printf("plugin %s exited unexpectedly", pi.Meta.ID)
				pi.TransitionTo(StateError,
					fmt.Errorf("%w: process terminated", ErrPluginCrashed))
				m.maybeAutoRestart(pi)
				return
			}
		case <-pi.stopCh:
			return
		case <-m.shutdownCh:
			return
		}
	}
}

// Shutdown stops all running plugins and releases resources.
func (m *Manager) Shutdown() error {
	close(m.shutdownCh)

	m.mu.RLock()
	ids := make([]string, 0, len(m.plugins))
	for id := range m.plugins {
		ids = append(ids, id)
	}
	m.mu.RUnlock()

	for _, id := range ids {
		pi := m.GetPlugin(id)
		if pi == nil {
			continue
		}
		if pi.State == StateRunning || pi.State == StateInitialized {
			if err := m.Stop(id); err != nil {
				m.log.Printf("error stopping %s during shutdown: %v", id, err)
			}
		}
	}

	// Stop all event delivery workers and clear subscriptions.
	m.events.close()

	return nil
}
