package hazel

import (
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-plugin"
)

// =========================================================================
// Test extensions
//
// echo is a host→plugin service (the plugin serves it, the host dispenses it).
// greet is a plugin→host service (the host serves it, the plugin consumes it).
// Both are registered in init() so the same test binary registers them in the
// host test process and in the re-executed plugin helper processes below.
// =========================================================================

const echoServiceName = "echo"
const greetServiceName = "greet"

// --- echo: host→plugin ----------------------------------------------------

type echoServer struct{}

func (echoServer) Echo(s string, out *string) error { *out = s + "!"; return nil }

type echoClient struct{ client *rpc.Client }

func (c *echoClient) Echo(s string) (string, error) {
	var out string
	if err := c.client.Call("Plugin.Echo", s, &out); err != nil {
		return "", err
	}
	return out, nil
}

type echoServicePlugin struct{}

func (echoServicePlugin) Server(_ *plugin.MuxBroker) (interface{}, error) {
	return &echoServer{}, nil
}

func (echoServicePlugin) Client(_ *plugin.MuxBroker, client *rpc.Client) (interface{}, error) {
	return &echoClient{client: client}, nil
}

// --- greet: plugin→host ---------------------------------------------------

// GreetAware lets a plugin receive the greet host service during Initialize.
type GreetAware interface {
	SetGreeter(greeter func(string) (string, error))
}

type greetServer struct{}

func (greetServer) Greet(s string, out *string) error { *out = "hello " + s; return nil }

type greetClient struct{ client *rpc.Client }

func (c *greetClient) Greet(s string) (string, error) {
	var out string
	if err := c.client.Call("Plugin.Greet", s, &out); err != nil {
		return "", err
	}
	return out, nil
}

func init() {
	RegisterPluginService(echoServiceName, echoServicePlugin{})

	RegisterHostService(HostService{
		Name:   greetServiceName,
		Server: func(m *Manager) any { return &greetServer{} },
		Client: func(client *rpc.Client) any { return &greetClient{client: client} },
		Inject: func(impl, client any) {
			if ga, ok := impl.(GreetAware); ok {
				ga.SetGreeter(client.(*greetClient).Greet)
			}
		},
	})
}

// greetPlugin implements Lifecycle and GreetAware. On Start it calls the
// injected greeter and writes the result to HAZEL_TEST_OUT.
type greetPlugin struct{ greeter func(string) (string, error) }

func (p *greetPlugin) SetGreeter(g func(string) (string, error)) { p.greeter = g }
func (p *greetPlugin) Initialize(InitializeArgs) error           { return nil }
func (p *greetPlugin) Stop() error                               { return nil }

func (p *greetPlugin) Start(StartArgs) error {
	if path := os.Getenv("HAZEL_TEST_OUT"); path != "" && p.greeter != nil {
		s, err := p.greeter("world")
		if err != nil {
			return err
		}
		return os.WriteFile(path, []byte(s), 0o644)
	}
	return nil
}

// TestExtensionHelperProcess is the re-exec target for the greet test.
func TestExtensionHelperProcess(t *testing.T) {
	if os.Getenv("HAZEL_TEST_EXTENSION") != "1" {
		return
	}
	Serve(&greetPlugin{})
}

// registerTestPlugin adds a plugin instance directly, bypassing Discover.
func registerTestPlugin(t *testing.T, m *Manager, id string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.plugins[id] = &PluginInstance{
		Meta:         PluginMeta{ID: id, Name: id, Version: "1.0.0", Type: PluginTypeNative, CmdName: filepath.Base(exe)},
		PluginDir:    filepath.Dir(exe),
		State:        StateUnloaded,
		stopCh:       make(chan struct{}),
		onTransition: m.onPluginTransition,
	}
	m.mu.Unlock()
}

// TestRegisterPluginService verifies the host→plugin direction: a registered
// plugin service can be dispensed and called across the process boundary.
func TestRegisterPluginService(t *testing.T) {
	cfg := DefaultManagerConfig()
	cfg.Command = func(execPath string) *exec.Cmd {
		cmd := exec.Command(execPath, "-test.run=TestHelperProcess")
		cmd.Env = append(os.Environ(), "HAZEL_TEST_PLUGIN=1")
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

	dispensed, err := m.Dispense("p", echoServiceName)
	if err != nil {
		t.Fatalf("dispense: %v", err)
	}
	echo := dispensed.(*echoClient)
	got, err := echo.Echo("hi")
	if err != nil {
		t.Fatalf("echo: %v", err)
	}
	if got != "hi!" {
		t.Errorf("Echo = %q, want %q", got, "hi!")
	}
}

// TestRegisterHostService verifies the plugin→host direction: a registered
// host service is served by the host and injected into the plugin, which can
// then call it.
func TestRegisterHostService(t *testing.T) {
	out := filepath.Join(t.TempDir(), "greet.txt")

	cfg := DefaultManagerConfig()
	cfg.Command = func(execPath string) *exec.Cmd {
		cmd := exec.Command(execPath, "-test.run=TestExtensionHelperProcess")
		cmd.Env = append(os.Environ(),
			"HAZEL_TEST_EXTENSION=1",
			"HAZEL_TEST_OUT="+out,
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

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read greet result: %v", err)
	}
	if got := string(b); got != "hello world" {
		t.Errorf("greet = %q, want %q", got, "hello world")
	}
}

// mustPanic asserts that f panics.
func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: expected panic", what)
		}
	}()
	f()
}

func TestRegisterDuplicateDetection(t *testing.T) {
	// A built-in plugin service name cannot be overridden.
	mustPanic(t, "built-in plugin name conflict", func() {
		RegisterPluginService(lifecyclePluginName, echoServicePlugin{})
	})

	// Registering the same plugin service name twice panics.
	RegisterPluginService("test-dup-plugin", echoServicePlugin{})
	mustPanic(t, "duplicate plugin service name", func() {
		RegisterPluginService("test-dup-plugin", echoServicePlugin{})
	})

	// An empty host service name is rejected.
	mustPanic(t, "empty host service name", func() {
		RegisterHostService(HostService{Name: ""})
	})

	// Registering the same host service name twice panics. The first entry is
	// a valid no-op so it is harmless to later lifecycle tests.
	noop := HostService{
		Name:   "test-dup-host",
		Server: func(_ *Manager) any { return &greetServer{} },
		Client: func(c *rpc.Client) any { return &greetClient{client: c} },
	}
	RegisterHostService(noop)
	mustPanic(t, "duplicate host service name", func() {
		RegisterHostService(noop)
	})
}
