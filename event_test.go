package hazel

import (
	"io"
	"log"
	"testing"
	"time"
)

func TestMatchTopic(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"a.b", "a.b", true},
		{"a.b", "a.c", false},
		{"a.b", "a.b.c", false},
		{"a.*", "a.b", true},
		{"a.*", "a.b.c", false},
		{"a.*", "a", false},
		{"a.>", "a.b", true},
		{"a.>", "a.b.c", true},
		{"a.>", "a", false},
		{"*.changed", "config.changed", true},
		{"*.changed", "config.reloaded", false},
		{">", "anything.here", true},
	}
	for _, c := range cases {
		if got := matchTopic(c.pattern, c.name); got != c.want {
			t.Errorf("matchTopic(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
}

// newTestBus builds an event bus with a discarded logger for unit tests.
func newTestBus() *eventBus {
	return newEventBus(log.New(io.Discard, "", 0))
}

func TestEventBusPublishSubscribe(t *testing.T) {
	b := newTestBus()

	got := make(chan Event, 1)
	id, err := b.Subscribe("config.changed", func(e Event) { got <- e })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// A matching event is delivered to the handler.
	b.Publish(Event{Name: "config.changed", Payload: "world"})
	select {
	case e := <-got:
		if e.Name != "config.changed" || e.Payload != "world" {
			t.Errorf("event = %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}

	// A non-matching event is not delivered.
	b.Publish(Event{Name: "other"})
	select {
	case e := <-got:
		t.Errorf("unexpected event delivered: %q", e.Name)
	case <-time.After(100 * time.Millisecond):
		// expected: nothing delivered
	}

	// Unsubscribe stops delivery.
	if err := b.Unsubscribe(id); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	b.Publish(Event{Name: "config.changed"})
	select {
	case e := <-got:
		t.Errorf("event delivered after unsubscribe: %q", e.Name)
	case <-time.After(100 * time.Millisecond):
		// expected: nothing delivered
	}
}

func TestEventBusUnsubscribeOwnership(t *testing.T) {
	b := newTestBus()

	id, err := b.subscribe("pluginA", "x", nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// A different owner cannot remove the subscription.
	if err := b.unsubscribe("pluginB", id); err == nil {
		t.Fatal("expected ownership error")
	}
	// The owner can.
	if err := b.unsubscribe("pluginA", id); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
}

func TestLifecycleEventPublished(t *testing.T) {
	m, err := NewManager(DefaultManagerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()

	got := make(chan Event, 1)
	if _, err := m.Events().Subscribe("plugin.running", func(e Event) { got <- e }); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	pi := &PluginInstance{
		Meta:         PluginMeta{ID: "p"},
		State:        StateInitialized,
		stopCh:       make(chan struct{}),
		onTransition: m.onPluginTransition,
	}
	if err := pi.TransitionTo(StateRunning, nil); err != nil {
		t.Fatalf("transition: %v", err)
	}

	select {
	case e := <-got:
		le, ok := e.Payload.(LifecycleEvent)
		if !ok || e.Name != "plugin.running" || e.Source != "p" || le.To != "running" {
			t.Errorf("event = %+v, want plugin.running from p with running payload", e)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lifecycle event")
	}
}
