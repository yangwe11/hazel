package hazel

import (
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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

// eventRecordingPlugin subscribes to "test.topic" via its Context and writes 
// each received event name to the file named by HAZEL_TEST_EVENTS.
type eventRecordingPlugin struct {
	bus EventBus
}

func (p *eventRecordingPlugin) SetContext(ctx Context) { p.bus = ctx.Bus() }

func (p *eventRecordingPlugin) Initialize(InitializeArgs) error {
	_, err := p.bus.Subscribe("test.topic", func(e Event) {
		os.WriteFile(os.Getenv("HAZEL_TEST_EVENTS"), []byte(e.Name), 0o644)
	})
	return err
}

func (p *eventRecordingPlugin) Start(StartArgs) error { return nil }
func (p *eventRecordingPlugin) Stop() error           { return nil }

// TestEventHelperProcess is the re-exec target for TestCrossProcessEventBus.
func TestEventHelperProcess(t *testing.T) {
	if os.Getenv("HAZEL_TEST_EVENT_PLUGIN") != "1" {
		return
	}
	Serve(&eventRecordingPlugin{})
}

// TestCrossProcessEventBus verifies the built-in event bus still works
// end-to-end after it moved onto its own broker connection: a plugin
// subscribes over the event host service, and the host delivers a published
// event back to it.
func TestCrossProcessEventBus(t *testing.T) {
	out := filepath.Join(t.TempDir(), "events.txt")

	cfg := DefaultManagerConfig()
	cfg.Command = func(execPath string) *exec.Cmd {
		cmd := exec.Command(execPath, "-test.run=TestEventHelperProcess")
		cmd.Env = append(os.Environ(),
			"HAZEL_TEST_EVENT_PLUGIN=1",
			"HAZEL_TEST_EVENTS="+out,
		)
		return cmd
	}

	m, err := NewManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()

	registerTestPlugin(t, m, "p")
	if err := m.Load("p"); err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := m.Initialize("p"); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if err := m.Start("p"); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Publish from the host; the plugin's subscription should deliver it back.
	if err := m.Events().Publish(Event{Name: "test.topic", Payload: "x"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Delivery is asynchronous, so poll briefly for the plugin to record it.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(out); err == nil && string(b) == "test.topic" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	b, _ := os.ReadFile(out)
	t.Fatalf("plugin did not receive event, got %q", string(b))
}
