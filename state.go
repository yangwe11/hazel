package hazel

import (
	"fmt"
	"time"
)

// PluginState represents a point in the plugin lifecycle.
type PluginState int

//go:generate stringer -type=PluginState
const (
	// StateUnloaded is the initial state after discovery, before Load().
	StateUnloaded PluginState = iota
	// StateLoaded means the plugin binary has been validated but the
	// process is not yet started.
	StateLoaded
	// StateInitialized means the plugin process is running and
	// Initialize() has been called successfully.
	StateInitialized
	// StateRunning means Start() has been called and the plugin is
	// fully operational.
	StateRunning
	// StateStopped means the plugin was stopped cleanly.
	StateStopped
	// StateError is a terminal state indicating a failure.
	StateError
)

// String returns a human-readable name for the state.
func (s PluginState) String() string {
	switch s {
	case StateUnloaded:
		return "unloaded"
	case StateLoaded:
		return "loaded"
	case StateInitialized:
		return "initialized"
	case StateRunning:
		return "running"
	case StateStopped:
		return "stopped"
	case StateError:
		return "error"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// allowedTransitions defines which state transitions are valid.
var allowedTransitions = map[PluginState]map[PluginState]bool{
	StateUnloaded:    {StateLoaded: true, StateError: true},
	StateLoaded:      {StateInitialized: true, StateError: true, StateStopped: true},
	StateInitialized: {StateRunning: true, StateStopped: true, StateError: true},
	StateRunning:     {StateStopped: true, StateError: true},
	StateStopped:     {},
	StateError:       {},
}

// CanTransition reports whether moving from current to next is a valid
// state transition.
func CanTransition(current, next PluginState) bool {
	allowed, ok := allowedTransitions[current]
	if !ok {
		return false
	}
	return allowed[next]
}

// StateChange records a plugin's transition from one state to another.
type StateChange struct {
	PluginID string
	From     PluginState
	To       PluginState
	Err      error
	Time     time.Time
}
