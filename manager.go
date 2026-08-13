package hazel

import (
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
	client    *plugin.Client
	rpcClient Lifecycle

	// started is true once Start has succeeded at least once; it drives
	// StartArgs.First across restarts.
	started bool

	stateMu   sync.Mutex
	changedAt time.Time
	listeners []chan StateChange

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

	log        *log.Logger
	shutdownCh chan struct{}
}

// NewManager creates a Manager. Plugins are registered via Discover before
// their lifecycle is driven.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	return &Manager{
		config:     cfg,
		plugins:    make(map[string]*PluginInstance),
		log:        log.New(os.Stdout, "[hazel] ", log.LstdFlags),
		shutdownCh: make(chan struct{}),
	}, nil
}

// =========================================================================
// Discovery
// =========================================================================

// Discover scans all configured plugin directories and adds discovered
// plugins to the registry in the Unloaded state. Plugins that are already
// registered (by ID) are skipped.
//
// If any plugin's engineRequirement is not satisfied by the running engine
// version, Discover registers nothing and returns an error joining every
// incompatibility. Returns the number of newly discovered plugins.
func (m *Manager) Discover() (int, error) {
	var all []PluginMeta
	for _, dir := range m.config.PluginDirs {
		metas, err := ScanDirectory(dir)
		if err != nil {
			return 0, fmt.Errorf("scan %s: %w", dir, err)
		}
		all = append(all, metas...)
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
			Meta:      meta,
			PluginDir: meta.pluginDir,
			State:     StateUnloaded,
			stopCh:    make(chan struct{}),
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
	pi.rpcClient = nil

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

	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: HandshakeConfig,
		Plugins: map[string]plugin.Plugin{
			lifecyclePluginName: &lifecyclePlugin{hostRPC: &hostRPCAdapter{manager: m}},
		},
		Cmd:        cmd,
		Managed:    true,
		SyncStdout: os.Stdout,
		SyncStderr: os.Stderr,
		Logger:     hclog.NewNullLogger(),
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
	pi.rpcClient = dispensed.(Lifecycle)

	// Initialize the plugin, passing host configuration and the host's RPC
	// endpoint so the plugin can call back to the host.
	args := InitializeArgs{
		Config: nil, // reserved for future use
	}
	if err := pi.rpcClient.Initialize(args); err != nil {
		pi.client.Kill()
		pi.TransitionTo(StateError, fmt.Errorf("initialize rpc: %w", err))
		return fmt.Errorf("initialize plugin %s: %w", pluginID, err)
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
	if err := pi.rpcClient.Start(StartArgs{First: first}); err != nil {
		pi.TransitionTo(StateError, fmt.Errorf("start rpc: %w", err))
		return fmt.Errorf("start plugin %s: %w", pluginID, err)
	}
	pi.started = true

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
	if pi.rpcClient != nil && (pi.State == StateInitialized || pi.State == StateRunning) {
		done := make(chan error, 1)
		go func() {
			done <- pi.rpcClient.Stop()
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

// =========================================================================
// Internal helpers
// =========================================================================

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

	return nil
}
