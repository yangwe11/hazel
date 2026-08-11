package hazel

import (
	"errors"
)

// Sentinel errors returned by the plugin manager and runtime.
var (
	// ErrDependencyCycle is returned when a circular dependency is detected
	// among plugins.
	ErrDependencyCycle = errors.New("plugin dependency cycle detected")

	// ErrDependencyMissing is returned when a required dependency is not
	// installed or not found in the registry.
	ErrDependencyMissing = errors.New("plugin dependency is not installed")

	// ErrVersionMismatch is returned when an installed dependency does not
	// satisfy the version constraint declared by the dependent plugin.
	ErrVersionMismatch = errors.New("plugin version constraint not satisfied")

	// ErrInvalidStateTransition is returned when an operation would cause
	// an invalid lifecycle state change (e.g., stopping an unloaded plugin).
	ErrInvalidStateTransition = errors.New("invalid plugin state transition")

	// ErrPluginCrashed is returned when a plugin process exits unexpectedly.
	ErrPluginCrashed = errors.New("plugin process exited unexpectedly")

	// ErrStartupTimeout is returned when a plugin fails to start within the
	// configured deadline.
	ErrStartupTimeout = errors.New("plugin startup timed out")

	// ErrStopTimeout is returned when a plugin fails to stop within the
	// configured deadline.
	ErrStopTimeout = errors.New("plugin stop timed out")

	// ErrPluginNotFound is returned when an operation references a plugin ID
	// that is not in the registry.
	ErrPluginNotFound = errors.New("plugin not found in registry")

	// ErrPluginAlreadyRunning is returned when attempting to start a plugin
	// that is already in the Running state.
	ErrPluginAlreadyRunning = errors.New("plugin is already running")

	// ErrHandshakeFailed is returned when the go-plugin handshake between
	// host and plugin fails.
	ErrHandshakeFailed = errors.New("plugin handshake failed")
)
