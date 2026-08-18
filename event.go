package hazel

import (
	"encoding/gob"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// =========================================================================
// Events
//
// The event bus is the host's hub-and-spoke router. Plugins and the host
// publish events and subscribe to topic patterns; the host fans each event
// out to every matching subscriber. Routing everything through the host keeps
// plugins decoupled: they never need to discover each other's addresses.
// =========================================================================

// Event is a single message on the bus.
//
// Payload may hold any Go value; it is gob-encoded when it crosses a process
// boundary. Any concrete type stored in Payload must be registered with
// gob.Register in every process that publishes or subscribes to it — including
// the host, which decodes and re-encodes the payload when forwarding between
// plugins. For loosely-coupled payloads that should not require shared
// registration, use a gob-basic type such as []byte (e.g. JSON-encoded).
type Event struct {
	Name    string    // topic, dot-separated, e.g. "config.changed"
	Source  string    // publisher plugin ID; the host fills it in when empty
	Payload any       // optional event data (see above)
	Time    time.Time // when the event was published (host-side)
}

// LifecycleEvent is the payload of the "plugin.*" events the host publishes
// automatically whenever a plugin changes state.
type LifecycleEvent struct {
	PluginID string
	From     string
	To       string
	Err      string // empty unless the transition records an error
}

// init registers built-in payload types so lifecycle events flow between the
// host and plugins without manual gob.Register calls.
func init() {
	gob.Register(LifecycleEvent{})
}

// EventBus is the event API exposed to plugins. Plugins opt in by
// implementing EventAware, mirroring how HostAware exposes HostRPC.
type EventBus interface {
	// Publish sends an event to every subscriber whose pattern matches Name.
	Publish(event Event) error

	// Subscribe registers handler for a topic pattern and returns a
	// subscription ID for later Unsubscribe. Handlers run in their own
	// goroutine and must not block the bus.
	Subscribe(pattern string, handler func(Event)) (string, error)

	// Unsubscribe removes the subscription with the given ID.
	Unsubscribe(id string) error
}

// =========================================================================
// Host-side bus
// =========================================================================

// eventBus is the host's event bus. It implements EventBus for host-local
// consumers and additionally tracks the delivery channel for each plugin
// process, so plugins receive events without the host knowing their handlers.
type eventBus struct {
	mu   sync.RWMutex
	subs map[string]*subscription
	seq  uint64

	// deliveries holds one entry per plugin process currently able to receive
	// events. Each entry is a buffered queue drained by its own goroutine, so
	// a slow plugin cannot stall publishers.
	deliveries map[string]*pluginDelivery

	wg  sync.WaitGroup
	log *log.Logger
}

// subscription is one subscriber's interest in a topic pattern.
type subscription struct {
	id      string
	owner   string // plugin ID, or "" for host-local subscriptions
	pattern string
	handler func(Event) // host-local subscriptions only
}

// pluginDelivery decouples the bus from a plugin's RPC client: publishers drop
// events into queue, and one goroutine forwards them via deliver.
type pluginDelivery struct {
	deliver func(Event) error
	queue   chan Event
	done    chan struct{}
}

// eventQueueSize bounds per-plugin delivery; overflow drops events so a slow
// plugin cannot stall publishers (mirrors PluginInstance.Listen).
const eventQueueSize = 8

func newEventBus(log *log.Logger) *eventBus {
	return &eventBus{
		subs:       make(map[string]*subscription),
		deliveries: make(map[string]*pluginDelivery),
		log:        log,
	}
}

// Publish sends an event to all matching subscribers (implements EventBus).
func (b *eventBus) Publish(event Event) error {
	b.publish(event)
	return nil
}

// Subscribe registers a host-local handler for a pattern (implements EventBus).
func (b *eventBus) Subscribe(pattern string, handler func(Event)) (string, error) {
	return b.subscribe("", pattern, handler)
}

// Unsubscribe removes a host-local subscription (implements EventBus).
func (b *eventBus) Unsubscribe(id string) error {
	return b.unsubscribe("", id)
}

// subscribe registers a subscription owned by owner ("" for the host).
func (b *eventBus) subscribe(owner, pattern string, handler func(Event)) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.seq++
	id := fmt.Sprintf("sub-%d", b.seq)
	b.subs[id] = &subscription{id: id, owner: owner, pattern: pattern, handler: handler}
	return id, nil
}

// unsubscribe removes a subscription, verifying the caller owns it.
func (b *eventBus) unsubscribe(owner, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub, ok := b.subs[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSubscriptionNotFound, id)
	}
	if sub.owner != owner {
		return fmt.Errorf("%w: %s is not owned by %q", ErrSubscriptionNotFound, id, owner)
	}
	delete(b.subs, id)
	return nil
}

// publish fans an event out to every matching subscriber. Host-local handlers
// run in their own goroutine; plugin deliveries are queued asynchronously.
func (b *eventBus) publish(event Event) {
	if event.Time.IsZero() {
		event.Time = time.Now()
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	// delivered dedups plugin deliveries so a plugin with several matching
	// subscriptions still receives the event only once (it re-matches locally).
	delivered := make(map[string]bool)
	for _, sub := range b.subs {
		if !matchTopic(sub.pattern, event.Name) {
			continue
		}
		if sub.owner == "" {
			go sub.handler(event)
			continue
		}
		if delivered[sub.owner] {
			continue
		}
		delivered[sub.owner] = true

		pd, ok := b.deliveries[sub.owner]
		if !ok {
			continue
		}
		select {
		case pd.queue <- event:
		default:
			b.log.Printf("event: dropped %q for %s (delivery queue full)", event.Name, sub.owner)
		}
	}
}

// registerPlugin wires a plugin's delivery function so the bus can push events
// to it. Called by the manager after dispensing the plugin's event client. If
// the plugin was already registered, the stale worker is replaced (restart).
func (b *eventBus) registerPlugin(pluginID string, deliver func(Event) error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if pd, ok := b.deliveries[pluginID]; ok {
		close(pd.done) // stop the stale worker; its pending events are dropped
		delete(b.deliveries, pluginID)
	}

	pd := &pluginDelivery{
		deliver: deliver,
		queue:   make(chan Event, eventQueueSize),
		done:    make(chan struct{}),
	}
	b.deliveries[pluginID] = pd

	b.wg.Add(1)
	go b.deliverLoop(pd)
}

// unregisterPlugin removes a plugin's subscriptions and stops its delivery
// worker. Called when a plugin stops or crashes.
func (b *eventBus) unregisterPlugin(pluginID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for id, sub := range b.subs {
		if sub.owner == pluginID {
			delete(b.subs, id)
		}
	}

	pd, ok := b.deliveries[pluginID]
	if !ok {
		return
	}
	delete(b.deliveries, pluginID)
	close(pd.done)
}

// close shuts down every delivery worker and clears subscriptions. Called on
// manager shutdown.
func (b *eventBus) close() {
	b.mu.Lock()
	for id, pd := range b.deliveries {
		close(pd.done)
		delete(b.deliveries, id)
	}
	b.subs = make(map[string]*subscription)
	b.mu.Unlock()

	b.wg.Wait()
}

// deliverLoop forwards queued events to a plugin until it is unregistered.
func (b *eventBus) deliverLoop(pd *pluginDelivery) {
	defer b.wg.Done()
	for {
		select {
		case event := <-pd.queue:
			if err := pd.deliver(event); err != nil {
				b.log.Printf("event: deliver failed: %v", err)
			}
		case <-pd.done:
			return
		}
	}
}

// =========================================================================
// Plugin-side: receive events and dispatch to local handlers
// =========================================================================

// pluginEventBus implements EventBus in the plugin process. It keeps handlers
// local (they never cross the process boundary) and forwards publish,
// subscribe, and unsubscribe calls to the host over HostRPC.
type pluginEventBus struct {
	host HostRPC // dialed during Initialize, before the plugin gets the bus

	mu       sync.RWMutex
	handlers map[string]pluginHandler // subscription ID → {pattern, handler}
}

// pluginHandler is a plugin-local event subscription.
type pluginHandler struct {
	pattern string
	handler func(Event)
}

// Publish sends an event to the host, which fans it out (implements EventBus).
func (b *pluginEventBus) Publish(event Event) error {
	return b.host.Publish(event, &Empty{})
}

// Subscribe registers a local handler and tells the host to route matching
// events to this plugin (implements EventBus).
func (b *pluginEventBus) Subscribe(pattern string, handler func(Event)) (string, error) {
	var id string
	if err := b.host.Subscribe(pattern, &id); err != nil {
		return "", err
	}

	b.mu.Lock()
	b.handlers[id] = pluginHandler{pattern: pattern, handler: handler}
	b.mu.Unlock()
	return id, nil
}

// Unsubscribe removes a local handler and tells the host to stop routing
// (implements EventBus).
func (b *pluginEventBus) Unsubscribe(id string) error {
	if err := b.host.Unsubscribe(id, &Empty{}); err != nil {
		return err
	}

	b.mu.Lock()
	delete(b.handlers, id)
	b.mu.Unlock()
	return nil
}

// dispatch runs every local handler whose pattern matches the event. It is
// invoked by the event RPC server when the host delivers an event.
func (b *pluginEventBus) dispatch(event Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, h := range b.handlers {
		if !matchTopic(h.pattern, event.Name) {
			continue
		}
		// Handlers run concurrently so one slow handler cannot block the rest.
		go h.handler(event)
	}
}

// matchTopic reports whether a topic name matches a subscription pattern.
// Both are dot-separated. In a pattern:
//
//	"*" matches exactly one segment
//	">" matches one or more trailing segments and must be last
//
// Any other segment must match literally.
func matchTopic(pattern, name string) bool {
	pp := strings.Split(pattern, ".")
	np := strings.Split(name, ".")

	for i, seg := range pp {
		switch {
		case seg == ">":
			return i < len(np)
		case i >= len(np):
			return false
		case seg == "*":
			continue
		case seg != np[i]:
			return false
		}
	}
	return len(pp) == len(np)
}
