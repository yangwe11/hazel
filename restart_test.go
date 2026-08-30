package hazel

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// crashPlugin bumps a start counter and then crashes shortly after, so Start
// succeeds and the crash is observed by the crash monitor (not as a Start RPC
// error).
type crashPlugin struct{}

func (crashPlugin) Initialize(InitializeArgs) error { return nil }

func (crashPlugin) Start(StartArgs) error {
	if path := os.Getenv("HAZEL_TEST_CRASH_COUNT"); path != "" {
		b, _ := os.ReadFile(path)
		n, _ := strconv.Atoi(string(b))
		os.WriteFile(path, []byte(strconv.Itoa(n+1)), 0o644)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		os.Exit(1)
	}()
	return nil
}

func (crashPlugin) Stop() error { return nil }

// TestCrashHelperProcess is the re-exec target for TestAutoRestart.
func TestCrashHelperProcess(t *testing.T) {
	if os.Getenv("HAZEL_TEST_CRASH") != "1" {
		return
	}
	Serve(&crashPlugin{})
}

// TestAutoRestart verifies that a crashed plugin is restarted automatically up
// to the configured budget, then left in StateError.
func TestAutoRestart(t *testing.T) {
	countFile := filepath.Join(t.TempDir(), "count")

	cfg := DefaultManagerConfig()
	cfg.Restart = &RestartPolicy{MaxRetries: 2, Backoff: 50 * time.Millisecond}
	cfg.Command = func(execPath string) *exec.Cmd {
		cmd := exec.Command(execPath, "-test.run=TestCrashHelperProcess")
		cmd.Env = append(os.Environ(),
			"HAZEL_TEST_CRASH=1",
			"HAZEL_TEST_CRASH_COUNT="+countFile,
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

	// The plugin crashes, is restarted MaxRetries times, then gives up and
	// settles in StateError. Wait for that settled condition.
	wantStarts := 1 + cfg.Restart.MaxRetries
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(countFile)
		n, _ := strconv.Atoi(string(b))
		if n >= wantStarts && m.GetPlugin("p").State == StateError {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	b, _ := os.ReadFile(countFile)
	t.Fatalf("plugin did not settle in error after %d starts (count=%q state=%s)",
		wantStarts, string(b), m.GetPlugin("p").State)
}
