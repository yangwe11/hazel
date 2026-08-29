// Command demo-plugin is a runnable demo plugin for hazel. It implements
// Lifecycle and ContextAware to exercise configuration, environment, the event
// bus, and runtime config updates.
package main

import (
	"encoding/json"
	"log"

	"github.com/yangwe11/hazel"
)

type demoPlugin struct{ ctx hazel.Context }

func (p *demoPlugin) SetContext(ctx hazel.Context) { p.ctx = ctx }

func (p *demoPlugin) Initialize(hazel.InitializeArgs) error {
	env := p.ctx.Environment()
	log.Printf("[demo] initialize: id=%s engine=%s dataDir=%s attrs=%v",
		p.ctx.ID(), env.EngineVersion, env.DataDir, env.Attributes)

	var cfg struct {
		Greeting string `json:"greeting"`
	}
	if err := json.Unmarshal(p.ctx.Config(), &cfg); err != nil {
		return err
	}
	log.Printf("[demo] config greeting=%q", cfg.Greeting)

	if _, err := p.ctx.Bus().Subscribe("demo.ping", func(e hazel.Event) {
		log.Printf("[demo] event %s: %v", e.Name, e.Payload)
	}); err != nil {
		return err
	}

	_, err := p.ctx.Bus().Subscribe(hazel.ConfigChangedTopic, func(e hazel.Event) {
		if cc, ok := e.Payload.(hazel.ConfigChanged); ok {
			log.Printf("[demo] config updated: %s", cc.Config)
		}
	})
	return err
}

func (p *demoPlugin) Start(hazel.StartArgs) error {
	log.Printf("[demo] started")
	return nil
}

func (p *demoPlugin) Stop() error {
	log.Printf("[demo] stopped")
	return nil
}

func main() {
	hazel.Serve(&demoPlugin{})
}
