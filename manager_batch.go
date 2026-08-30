package hazel

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// =========================================================================
// Dependency resolution
// =========================================================================

// dependencyGraph models plugin dependencies for topological sort.
type dependencyGraph struct {
	nodes map[string]*graphNode // plugin ID → node
}

type graphNode struct {
	meta       PluginMeta
	dependents []string // plugins that depend on this one (reverse edges)
	depCount   int      // number of unmet (non-optional) dependencies
}

// buildGraph constructs a dependency graph from plugin metadata.
// Optional dependencies are included for ordering only if the target
// plugin is present in the registry.
func buildGraph(plugins map[string]PluginMeta) *dependencyGraph {
	g := &dependencyGraph{
		nodes: make(map[string]*graphNode, len(plugins)),
	}

	// Create nodes.
	for id, meta := range plugins {
		g.nodes[id] = &graphNode{meta: meta}
	}

	// Wire edges.
	for id, node := range g.nodes {
		for _, dep := range node.meta.Depends {
			depNode, ok := g.nodes[dep.ID]
			if !ok {
				if dep.Optional {
					continue // optional dep not installed — skip
				}
			}
			// Required dep (or optional that is present).
			if depNode != nil {
				depNode.dependents = append(depNode.dependents, id)
			}
			if !dep.Optional {
				node.depCount++
			}
		}
	}

	return g
}

// resolveOrder performs a Kahn topological sort and returns ordered
// batches. Plugins within the same batch have no dependencies on each
// other and can be started concurrently.
func (g *dependencyGraph) resolveOrder() ([][]string, error) {
	// Copy depCounts so we can mutate them.
	remaining := make(map[string]int, len(g.nodes))
	for id, node := range g.nodes {
		remaining[id] = node.depCount
	}

	var batches [][]string

	// Seed queue with nodes that have zero unmet dependencies.
	queue := make([]string, 0)
	for id, count := range remaining {
		if count == 0 {
			queue = append(queue, id)
		}
	}

	for len(queue) > 0 {
		// Current batch = everything in the queue right now.
		batch := make([]string, len(queue))
		copy(batch, queue)
		batches = append(batches, batch)

		// Process batch: decrement dependents' depCounts.
		nextQueue := make([]string, 0)
		for _, id := range queue {
			node := g.nodes[id]
			for _, depID := range node.dependents {
				remaining[depID]--
				if remaining[depID] == 0 {
					nextQueue = append(nextQueue, depID)
				}
			}
		}
		queue = nextQueue
	}

	// Check for cycles: any node with non-zero depCount.
	var unprocessed []string
	for id, count := range remaining {
		if count > 0 {
			unprocessed = append(unprocessed, id)
		}
	}
	if len(unprocessed) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrDependencyCycle,
			strings.Join(unprocessed, ", "))
	}

	return batches, nil
}

// validateDependencyVersions checks that every non-optional dependency's
// installed version satisfies the required version constraint.
func validateDependencyVersions(plugins map[string]PluginMeta) error {
	for _, meta := range plugins {
		for _, dep := range meta.Depends {
			if dep.Optional {
				continue
			}
			depMeta, ok := plugins[dep.ID]
			if !ok {
				return fmt.Errorf("%w: %s depends on %s, which is not installed",
					ErrDependencyMissing, meta.ID, dep.ID)
			}
			if dep.Requirement != "" {
				if !Match(string(dep.Requirement), depMeta.Version) {
					return fmt.Errorf("%w: %s requires %s %s, but version %s is installed",
						ErrVersionMismatch, meta.ID, dep.ID,
						dep.Requirement, depMeta.Version)
				}
			}
		}
	}
	return nil
}

// =========================================================================
// StartAll / StopAll
// =========================================================================

// StartAllResult summarizes the outcome of a StartAll call.
type StartAllResult struct {
	Started []string         // plugin IDs that reached Running
	Failed  []StartupFailure // plugins that failed
	Skipped []string         // plugins skipped due to dependency failure
}

// StartupFailure describes a plugin that failed during startup.
type StartupFailure struct {
	PluginID string
	Stage    string // "load", "initialize", or "start"
	Error    error
}

// StartAll resolves the dependency graph, validates version constraints,
// and starts all registered plugins in dependency order. Plugins within
// the same dependency batch start concurrently.
//
// If a dependency failure prevents a plugin from starting, dependent
// plugins in later batches are skipped rather than started.
func (m *Manager) StartAll(ctx context.Context) (*StartAllResult, error) {
	m.mu.RLock()
	// Build a snapshot of plugin metadata.
	metaMap := make(map[string]PluginMeta, len(m.plugins))
	for id, pi := range m.plugins {
		metaMap[id] = pi.Meta
	}
	m.mu.RUnlock()

	// Validate version constraints.
	if err := validateDependencyVersions(metaMap); err != nil {
		return nil, err
	}

	// Build and resolve dependency graph.
	graph := buildGraph(metaMap)
	batches, err := graph.resolveOrder()
	if err != nil {
		return nil, err
	}

	result := &StartAllResult{}

	for _, batch := range batches {
		// Filter out plugins that are already running.
		var pending []string
		for _, id := range batch {
			pi := m.GetPlugin(id)
			if pi != nil && pi.State == StateRunning {
				result.Started = append(result.Started, id)
				continue
			}
			pending = append(pending, id)
		}

		if len(pending) == 0 {
			continue
		}

		// Start plugins in this batch concurrently.
		batchErrs := m.startBatch(ctx, pending)
		failedSet := make(map[string]bool, len(batchErrs))
		for _, be := range batchErrs {
			failedSet[be.PluginID] = true
		}
		for _, id := range pending {
			if failedSet[id] {
				// Mark dependents as skipped.
				node := graph.nodes[id]
				if node != nil {
					for _, depID := range node.dependents {
						result.Skipped = append(result.Skipped, depID)
					}
				}
			} else {
				result.Started = append(result.Started, id)
			}
		}

		for _, be := range batchErrs {
			result.Failed = append(result.Failed, be)
		}
	}

	return result, nil
}

// StopAll stops all running plugins in reverse dependency order so that
// plugins that depend on others are stopped before the plugins they
// depend on.
func (m *Manager) StopAll(ctx context.Context) error {
	m.mu.RLock()
	metaMap := make(map[string]PluginMeta, len(m.plugins))
	for id, pi := range m.plugins {
		metaMap[id] = pi.Meta
	}
	m.mu.RUnlock()

	graph := buildGraph(metaMap)
	batches, err := graph.resolveOrder()
	if err != nil {
		// If there's a cycle, stop everything individually.
		m.logger.Warn("dependency cycle detected during stop; stopping all plugins individually")
		for id := range metaMap {
			pi := m.GetPlugin(id)
			if pi != nil && (pi.State == StateRunning || pi.State == StateInitialized) {
				m.Stop(id)
			}
		}
		return nil
	}

	// Reverse the order: stop in reverse dependency order.
	for i := len(batches) - 1; i >= 0; i-- {
		var wg sync.WaitGroup
		for _, id := range batches[i] {
			pi := m.GetPlugin(id)
			if pi == nil || (pi.State != StateRunning && pi.State != StateInitialized) {
				continue
			}
			wg.Add(1)
			go func(pluginID string) {
				defer wg.Done()
				if err := m.Stop(pluginID); err != nil {
					m.logger.Warn("error stopping plugin", "plugin", pluginID, "error", err)
				}
			}(id)
		}
		wg.Wait()
	}

	return nil
}

// =========================================================================
// Batch startup
// =========================================================================

// startBatch launches all plugins in a batch concurrently and waits for
// each to complete the Load→Initialize→Start sequence. It returns any
// errors encountered.
func (m *Manager) startBatch(ctx context.Context, batch []string) []StartupFailure {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []StartupFailure
		sem  chan struct{}
	)

	if m.config.MaxParallel > 0 {
		sem = make(chan struct{}, m.config.MaxParallel)
	}

	for _, id := range batch {
		wg.Add(1)
		go func(pluginID string) {
			defer wg.Done()

			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}

			startCtx, cancel := context.WithTimeout(ctx, m.config.StartTimeout)
			defer cancel()

			if sf := m.startOne(startCtx, pluginID); sf != nil {
				mu.Lock()
				errs = append(errs, *sf)
				mu.Unlock()
			}
		}(id)
	}

	wg.Wait()
	return errs
}

// startOne runs the full Load→Initialize→Start sequence for a single
// plugin. It returns nil on success, or a StartupFailure on error.
func (m *Manager) startOne(ctx context.Context, pluginID string) *StartupFailure {
	done := make(chan *StartupFailure, 1)

	go func() {
		if err := m.Load(pluginID); err != nil {
			done <- &StartupFailure{PluginID: pluginID, Stage: "load", Error: err}
			return
		}
		if err := m.Initialize(pluginID); err != nil {
			done <- &StartupFailure{PluginID: pluginID, Stage: "initialize", Error: err}
			return
		}
		if err := m.Start(pluginID); err != nil {
			done <- &StartupFailure{PluginID: pluginID, Stage: "start", Error: err}
			return
		}
		done <- nil
	}()

	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		return &StartupFailure{
			PluginID: pluginID,
			Stage:    "timeout",
			Error:    fmt.Errorf("%w: %s", ErrStartupTimeout, ctx.Err()),
		}
	}
}
