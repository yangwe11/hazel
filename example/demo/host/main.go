// Command demo-host is the host side of the runnable hazel demo. It discovers
// the demo plugin, drives its lifecycle, publishes an event, and pushes a
// runtime configuration update.
package main

import (
	"log"
	"time"

	"github.com/yangwe11/hazel"
)

func main() {
	m, err := hazel.NewManager(hazel.ManagerConfig{
		PluginDirs: []string{"plugins"},
		DataDir:    "data",
		Attributes: map[string]string{"environment": "demo"},
		Config: func(id string) (any, error) {
			return map[string]any{"greeting": "hello from host"}, nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer m.Shutdown()

	if n, err := m.Discover(); err != nil {
		log.Fatal(err)
	} else {
		log.Printf("[host] discovered %d plugin(s)", n)
	}

	for _, id := range []string{"demo"} {
		if err := m.Load(id); err != nil {
			log.Fatalf("[host] load %s: %v", id, err)
		}
		if err := m.Initialize(id); err != nil {
			log.Fatalf("[host] initialize %s: %v", id, err)
		}
		if err := m.Start(id); err != nil {
			log.Fatalf("[host] start %s: %v", id, err)
		}
	}

	// Publish an event the plugin subscribed to, then push a runtime config
	// update (delivered as a config.changed event).
	m.Events().Publish(hazel.Event{Name: "demo.ping", Payload: "ping"})
	if err := m.UpdateConfig("demo", map[string]any{"greeting": "updated greeting"}); err != nil {
		log.Fatal(err)
	}

	// Give the plugin time to log the event and the update before shutdown.
	time.Sleep(2 * time.Second)
}
