package hazel

import (
	"bytes"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

// logPlugin writes a line via the standard log package during Start. The line
// carries no level prefix, so go-plugin classifies it as Debug.
type logPlugin struct{}

func (logPlugin) Initialize(InitializeArgs) error { return nil }
func (logPlugin) Stop() error                     { return nil }

func (logPlugin) Start(StartArgs) error {
	log.Printf("hello from plugin")
	return nil
}

// TestLogPluginHelperProcess is the re-exec target for TestPluginLogCollected.
func TestLogPluginHelperProcess(t *testing.T) {
	if os.Getenv("HAZEL_TEST_LOG_PLUGIN") != "1" {
		return
	}
	Serve(&logPlugin{})
}

// syncBuffer is a concurrency-safe bytes.Buffer for capturing hclog output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestPluginLogCollected verifies that a plugin's stderr output is read by
// go-plugin and merged into the host's logger.
func TestPluginLogCollected(t *testing.T) {
	var buf syncBuffer

	cfg := DefaultManagerConfig()
	cfg.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "hazel",
		Level:  hclog.Debug,
		Output: &buf,
	})
	cfg.Command = func(execPath string) *exec.Cmd {
		cmd := exec.Command(execPath, "-test.run=TestLogPluginHelperProcess")
		cmd.Env = append(os.Environ(), "HAZEL_TEST_LOG_PLUGIN=1")
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

	// The plugin's log line is forwarded asynchronously, so poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "hello from plugin") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("plugin log not collected, got %q", buf.String())
}
