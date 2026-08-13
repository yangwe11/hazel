package hazel

import (
	"errors"
)

// Sentinel errors returned by the plugin manager and runtime.
var (
	// ErrDependencyCycle indicates a circular dependency among plugins.
	ErrDependencyCycle = errors.New("plugin dependency cycle detected")

	// ErrDependencyMissing indicates a required dependency is not installed.
	ErrDependencyMissing = errors.New("plugin dependency is not installed")

	// ErrVersionMismatch indicates an installed dependency does not satisfy
	// the version constraint declared by the dependent plugin.
	ErrVersionMismatch = errors.New("plugin version constraint not satisfied")

	// ErrEngineMismatch indicates a plugin's engineRequirement is not satisfied
	// by the running engine version.
	ErrEngineMismatch = errors.New("plugin requires a different engine version")

	// ErrDuplicatePluginID indicates two discovered plugins share the same ID.
	ErrDuplicatePluginID = errors.New("duplicate plugin ID")

	// ErrInvalidStateTransition indicates an operation would move a plugin
	// through an invalid lifecycle transition.
	ErrInvalidStateTransition = errors.New("invalid plugin state transition")

	// ErrPluginCrashed indicates a plugin process exited unexpectedly.
	ErrPluginCrashed = errors.New("plugin process exited unexpectedly")

	// ErrStartupTimeout indicates a plugin failed to start within the
	// configured deadline.
	ErrStartupTimeout = errors.New("plugin startup timed out")

	// ErrPluginNotFound indicates an operation referenced an unregistered ID.
	ErrPluginNotFound = errors.New("plugin not found in registry")
)
