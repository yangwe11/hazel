package hazel

import "testing"

func TestCanTransition(t *testing.T) {
	tests := []struct {
		from, to PluginState
		want     bool
	}{
		// Forward lifecycle.
		{StateUnloaded, StateLoaded, true},
		{StateLoaded, StateInitialized, true},
		{StateInitialized, StateRunning, true},

		// Restart edges (the point of this test).
		{StateStopped, StateLoaded, true},
		{StateError, StateLoaded, true},

		// Error edges.
		{StateUnloaded, StateError, true},
		{StateLoaded, StateError, true},
		{StateInitialized, StateError, true},
		{StateRunning, StateError, true},

		// Cleanup.
		{StateLoaded, StateStopped, true},
		{StateInitialized, StateStopped, true},
		{StateRunning, StateStopped, true},

		// Invalid transitions.
		{StateUnloaded, StateInitialized, false},
		{StateUnloaded, StateRunning, false},
		{StateUnloaded, StateStopped, false},
		{StateLoaded, StateRunning, false},
		{StateInitialized, StateLoaded, false},
		{StateRunning, StateLoaded, false},
		{StateRunning, StateInitialized, false},
		{StateRunning, StateUnloaded, false},
		{StateStopped, StateInitialized, false},
		{StateStopped, StateRunning, false},
		{StateStopped, StateStopped, false},
		{StateError, StateError, false},
		{StateError, StateStopped, false},
		{StateError, StateRunning, false},
	}

	for _, tt := range tests {
		if got := CanTransition(tt.from, tt.to); got != tt.want {
			t.Errorf("CanTransition(%s, %s) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

func TestPluginStateString(t *testing.T) {
	cases := map[PluginState]string{
		StateUnloaded:    "unloaded",
		StateLoaded:      "loaded",
		StateInitialized: "initialized",
		StateRunning:     "running",
		StateStopped:     "stopped",
		StateError:       "error",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("PluginState(%d).String() = %q, want %q", s, got, want)
		}
	}
	if got := PluginState(99).String(); got != "unknown(99)" {
		t.Errorf("unknown PluginState.String() = %q, want %q", got, "unknown(99)")
	}
}
