package hazel

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// configPlugin records its Context config to the file named by HAZEL_TEST_CONFIG
// during Initialize, and writes each config.changed payload to the file named
// by HAZEL_TEST_CONFIG_UPDATES.
type configPlugin struct {
	ctx Context
}

func (p *configPlugin) SetContext(ctx Context) { p.ctx = ctx }

func (p *configPlugin) Initialize(InitializeArgs) error {
	if err := os.WriteFile(os.Getenv("HAZEL_TEST_CONFIG"), p.ctx.Config(), 0o644); err != nil {
		return err
	}
	_, err := p.ctx.Bus().Subscribe(ConfigChangedTopic, func(e Event) {
		cc, ok := e.Payload.(ConfigChanged)
		if !ok {
			return
		}
		os.WriteFile(os.Getenv("HAZEL_TEST_CONFIG_UPDATES"), cc.Config, 0o644)
	})
	return err
}

func (p *configPlugin) Start(StartArgs) error { return nil }
func (p *configPlugin) Stop() error           { return nil }

// TestConfigHelperProcess is the re-exec target for TestConfigPushDown.
func TestConfigHelperProcess(t *testing.T) {
	if os.Getenv("HAZEL_TEST_CONFIG_PLUGIN") != "1" {
		return
	}
	Serve(&configPlugin{})
}

// TestConfigPushDown verifies both halves of configuration delivery: the static
// config pushed at Initialize (via Context.Config), and a runtime update pushed
// through a config.changed event.
func TestConfigPushDown(t *testing.T) {
	cfgOut := filepath.Join(t.TempDir(), "config.json")
	updOut := filepath.Join(t.TempDir(), "updates.json")

	cfg := DefaultManagerConfig()
	cfg.Config = func(id string) (any, error) {
		return map[string]any{
			"name":   id,
			"port":   8080,
			"nested": map[string]any{"enabled": true},
		}, nil
	}
	cfg.Command = func(execPath string) *exec.Cmd {
		cmd := exec.Command(execPath, "-test.run=TestConfigHelperProcess")
		cmd.Env = append(os.Environ(),
			"HAZEL_TEST_CONFIG_PLUGIN=1",
			"HAZEL_TEST_CONFIG="+cfgOut,
			"HAZEL_TEST_CONFIG_UPDATES="+updOut,
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

	// The static config reached the plugin via Context.Config().
	b, err := os.ReadFile(cfgOut)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	if got["name"] != "p" || got["port"] != float64(8080) {
		t.Fatalf("config = %v, want name=p port=8080", got)
	}
	if nested, ok := got["nested"].(map[string]any); !ok || nested["enabled"] != true {
		t.Fatalf("config nested = %v, want enabled=true", got["nested"])
	}

	if err := m.Start("p"); err != nil {
		t.Fatalf("start: %v", err)
	}

	// A runtime update reaches the plugin via a config.changed event.
	if err := m.UpdateConfig("p", map[string]any{"port": 9090}); err != nil {
		t.Fatalf("update config: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(updOut); err == nil {
			var upd map[string]any
			if json.Unmarshal(b, &upd) == nil && upd["port"] == float64(9090) {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	b2, _ := os.ReadFile(updOut)
	t.Fatalf("plugin did not receive config update, got %q", string(b2))
}

// envPlugin writes its Context.Environment() (JSON-encoded) to the file named
// by HAZEL_TEST_INFO during Initialize.
type envPlugin struct {
	ctx Context
}

func (p *envPlugin) SetContext(ctx Context) { p.ctx = ctx }

func (p *envPlugin) Initialize(InitializeArgs) error {
	data, err := json.Marshal(p.ctx.Environment())
	if err != nil {
		return err
	}
	return os.WriteFile(os.Getenv("HAZEL_TEST_INFO"), data, 0o644)
}

func (p *envPlugin) Start(StartArgs) error { return nil }
func (p *envPlugin) Stop() error           { return nil }

// TestEnvironmentHelperProcess is the re-exec target for TestEnvironmentPushDown.
func TestEnvironmentHelperProcess(t *testing.T) {
	if os.Getenv("HAZEL_TEST_INFO_PLUGIN") != "1" {
		return
	}
	Serve(&envPlugin{})
}

// TestEnvironmentPushDown verifies that shared host facts (engine version, data
// directory, and host-defined attributes) reach a plugin via Context.Environment().
func TestEnvironmentPushDown(t *testing.T) {
	infoOut := filepath.Join(t.TempDir(), "info.json")

	cfg := DefaultManagerConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")
	cfg.Attributes = map[string]string{"environment": "staging"}
	cfg.Command = func(execPath string) *exec.Cmd {
		cmd := exec.Command(execPath, "-test.run=TestEnvironmentHelperProcess")
		cmd.Env = append(os.Environ(),
			"HAZEL_TEST_INFO_PLUGIN=1",
			"HAZEL_TEST_INFO="+infoOut,
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

	b, err := os.ReadFile(infoOut)
	if err != nil {
		t.Fatalf("read info: %v", err)
	}
	var env Environment
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("unmarshal info: %v", err)
	}
	if env.EngineVersion != EngineVersion {
		t.Errorf("EngineVersion = %q, want %q", env.EngineVersion, EngineVersion)
	}
	if env.DataDir != cfg.DataDir {
		t.Errorf("DataDir = %q, want %q", env.DataDir, cfg.DataDir)
	}
	if env.Attributes["environment"] != "staging" {
		t.Errorf("Attributes = %v, want environment=staging", env.Attributes)
	}
}
