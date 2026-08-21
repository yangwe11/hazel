package hazel

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

// TestHelperProcess is the re-exec target for the integration test below. When
// run with HAZEL_TEST_PLUGIN=1 (via -test.run=TestHelperProcess) it serves
// recordingPlugin; otherwise it is a no-op that lets the normal suite pass.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("HAZEL_TEST_PLUGIN") != "1" {
		return
	}
	Serve(&recordingPlugin{})
}

// recordingPlugin records whether StartArgs.First was set by writing it to the
// file named by HAZEL_TEST_OUT.
type recordingPlugin struct{}

func (recordingPlugin) Initialize(InitializeArgs) error { return nil }

func (recordingPlugin) Start(args StartArgs) error {
	if path := os.Getenv("HAZEL_TEST_OUT"); path != "" {
		return os.WriteFile(path, []byte(strconv.FormatBool(args.First)), 0o644)
	}
	return nil
}

func (recordingPlugin) Stop() error { return nil }

// TestManagerRestartAndFirst runs a full lifecycle against a real plugin
// process (the test binary itself in helper mode) and verifies that:
//   - the plugin reaches Running, then Stopped, then Running again; and
//   - StartArgs.First is true on the first start and false on the restart.
func TestManagerRestartAndFirst(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(t.TempDir(), "first.txt")

	cfg := DefaultManagerConfig()
	cfg.Command = func(execPath string) *exec.Cmd {
		cmd := exec.Command(execPath, "-test.run=TestHelperProcess")
		cmd.Env = append(os.Environ(),
			"HAZEL_TEST_PLUGIN=1",
			"HAZEL_TEST_OUT="+out,
		)
		return cmd
	}

	m, err := NewManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()

	// Register a plugin instance directly, bypassing Discover so we don't need
	// a plugin.yaml on disk. The plugin binary is the test binary itself.
	m.mu.Lock()
	m.plugins["p"] = &PluginInstance{
		Meta:      PluginMeta{ID: "p", Name: "p", Version: "1.0.0", Type: PluginTypeNative, CmdName: filepath.Base(exe)},
		PluginDir: filepath.Dir(exe),
		State:     StateUnloaded,
		stopCh:    make(chan struct{}),
	}
	m.mu.Unlock()

	start := func() {
		t.Helper()
		if err := m.Load("p"); err != nil {
			t.Fatalf("load: %v", err)
		}
		if err := m.Initialize("p"); err != nil {
			t.Fatalf("initialize: %v", err)
		}
		if err := m.Start("p"); err != nil {
			t.Fatalf("start: %v", err)
		}
	}

	readFirst := func() string {
		t.Helper()
		b, err := os.ReadFile(out)
		if err != nil {
			t.Fatalf("read first flag: %v", err)
		}
		return string(b)
	}

	// First run.
	start()
	if got := readFirst(); got != "true" {
		t.Fatalf("first start First = %q, want true", got)
	}
	if got := m.GetPlugin("p").State; got != StateRunning {
		t.Fatalf("state after first start = %s, want running", got)
	}

	// Stop.
	if err := m.Stop("p"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if got := m.GetPlugin("p").State; got != StateStopped {
		t.Fatalf("state after stop = %s, want stopped", got)
	}

	// Restart: the Stopped→Loaded edge must permit a reload, and First must
	// now be false.
	start()
	if got := readFirst(); got != "false" {
		t.Fatalf("restart First = %q, want false", got)
	}
	if got := m.GetPlugin("p").State; got != StateRunning {
		t.Fatalf("state after restart = %s, want running", got)
	}
}
