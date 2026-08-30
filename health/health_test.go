package health

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/yangwe11/hazel"
)

// healthPlugin reports ready during Initialize via its injected Reporter.
type healthPlugin struct {
	reporter *Reporter
}

func (p *healthPlugin) SetReporter(r *Reporter) { p.reporter = r }

func (p *healthPlugin) Initialize(hazel.InitializeArgs) error {
	if p.reporter != nil {
		return p.reporter.Ready("up")
	}
	return nil
}

func (p *healthPlugin) Start(hazel.StartArgs) error { return nil }
func (p *healthPlugin) Stop() error                 { return nil }

// TestHealthHelperProcess is the re-exec target for TestHealthCapability.
func TestHealthHelperProcess(t *testing.T) {
	if os.Getenv("HAZEL_TEST_HEALTH") != "1" {
		return
	}
	hazel.Serve(&healthPlugin{})
}

// TestHealthCapability verifies the health capability end-to-end: a plugin
// reports ready through the host service (registered via the extension
// registry), and host code queries the reported status.
func TestHealthCapability(t *testing.T) {
	dir := t.TempDir()
	pluginDir := filepath.Join(dir, "p")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "plugin.yaml"),
		[]byte("id: p\nname: p\nversion: 1.0.0\ntype: NATIVE\ncmdName: p\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := hazel.DefaultManagerConfig()
	cfg.PluginDirs = []string{dir}
	cfg.Command = func(_ string) *exec.Cmd {
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(exe, "-test.run=TestHealthHelperProcess")
		cmd.Env = append(os.Environ(), "HAZEL_TEST_HEALTH=1")
		return cmd
	}

	m, err := hazel.NewManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()

	if _, err := m.Discover(); err != nil {
		t.Fatalf("discover: %v", err)
	}
	if err := m.Load("p"); err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := m.Initialize("p"); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	if !Ready("p") {
		t.Fatal("plugin p should have reported ready")
	}
	if s, ok := StatusOf("p"); !ok || s.Ready != true || s.Msg != "up" {
		t.Fatalf("status = %+v, ok=%v, want ready with msg up", s, ok)
	}
}
