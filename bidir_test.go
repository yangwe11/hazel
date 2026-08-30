package hazel

import (
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hashicorp/go-plugin"
)

// =========================================================================
// A bidirectional "ping" capability, built entirely through the extension
// registry (no kernel changes):
//
//   - pingPluginName (host→plugin): the plugin serves it, the host consumes it
//     via Manager.Dispense.
//   - pingHostName (plugin→host): the host serves it with a per-plugin receiver
//     (the new Server signature), the plugin consumes it via a *Aware
//     interface, and the host binds the host→plugin client via Wire.
// =========================================================================

const pingPluginName = "ping-plugin"
const pingHostName = "ping-host"

// --- host→plugin: plugin serves, host dispenses ---

type pingPluginServer struct{}

func (pingPluginServer) Ping(_ string, out *string) error { *out = "pong from plugin"; return nil }

type pingPluginClient struct{ client *rpc.Client }

func (c *pingPluginClient) Ping() (string, error) {
	var out string
	if err := c.client.Call("Plugin.Ping", "", &out); err != nil {
		return "", err
	}
	return out, nil
}

type pingPluginService struct{}

func (pingPluginService) Server(_ *plugin.MuxBroker) (interface{}, error) {
	return &pingPluginServer{}, nil
}

func (pingPluginService) Client(_ *plugin.MuxBroker, client *rpc.Client) (interface{}, error) {
	return &pingPluginClient{client: client}, nil
}

// --- plugin→host: host serves (per-plugin), plugin consumes via *Aware ---

// pingHostState is the host-side per-plugin state the capability keeps so the
// Server (plugin→host) and Wire (host→plugin) directions can share it.
var (
	pingMu    sync.Mutex
	pingState = map[string]*pingHostState{}
)

type pingHostState struct {
	pluginClient *pingPluginClient // bound by Wire during Initialize
}

// pingHostServer serves the plugin→host direction. It receives the plugin ID
// via the Server signature, so it can echo which plugin is connected.
type pingHostServer struct{ pluginID string }

func (s pingHostServer) Ping(_ string, out *string) error {
	*out = "pong from host to " + s.pluginID
	return nil
}

// pingHostClient is the plugin-side client for the host's ping service.
type pingHostClient struct{ client *rpc.Client }

func (c *pingHostClient) Ping() (string, error) {
	var out string
	if err := c.client.Call("Plugin.Ping", "", &out); err != nil {
		return "", err
	}
	return out, nil
}

// PingAware lets a plugin receive the host's ping client during Initialize.
type PingAware interface {
	SetPinger(func() (string, error))
}

func init() {
	RegisterPluginService(pingPluginName, pingPluginService{})

	RegisterHostService(HostService{
		Name:   pingHostName,
		Server: func(_ *Manager, pluginID string) any { return &pingHostServer{pluginID: pluginID} },
		Client: func(client *rpc.Client) any { return &pingHostClient{client: client} },
		Inject: func(impl, client any) {
			if p, ok := impl.(PingAware); ok {
				p.SetPinger(client.(*pingHostClient).Ping)
			}
		},
		Wire: func(m *Manager, pluginID string) error {
			dispensed, err := m.Dispense(pluginID, pingPluginName)
			if err != nil {
				return err
			}
			pingMu.Lock()
			pingState[pluginID] = &pingHostState{pluginClient: dispensed.(*pingPluginClient)}
			pingMu.Unlock()
			return nil
		},
	})
}

// pingPlugin implements Lifecycle and PingAware. During Initialize it calls the
// host's ping service and records the reply.
type pingPlugin struct {
	pinger func() (string, error)
}

func (p *pingPlugin) SetPinger(f func() (string, error)) { p.pinger = f }

func (p *pingPlugin) Initialize(InitializeArgs) error {
	if p.pinger == nil {
		return nil
	}
	out, err := p.pinger()
	if err != nil {
		return err
	}
	return os.WriteFile(os.Getenv("HAZEL_TEST_PING_HOST"), []byte(out), 0o644)
}

func (p *pingPlugin) Start(StartArgs) error { return nil }
func (p *pingPlugin) Stop() error           { return nil }

// TestPingHelperProcess is the re-exec target for TestBidirectionalCapability.
func TestPingHelperProcess(t *testing.T) {
	if os.Getenv("HAZEL_TEST_PING") != "1" {
		return
	}
	Serve(&pingPlugin{})
}

// TestBidirectionalCapability verifies both directions of a capability built
// entirely through the registry: the plugin→host direction carries the plugin
// ID (via the Server signature), and the host→plugin direction is auto-wired
// (via Wire).
func TestBidirectionalCapability(t *testing.T) {
	out := filepath.Join(t.TempDir(), "ping-host.txt")

	cfg := DefaultManagerConfig()
	cfg.Command = func(execPath string) *exec.Cmd {
		cmd := exec.Command(execPath, "-test.run=TestPingHelperProcess")
		cmd.Env = append(os.Environ(),
			"HAZEL_TEST_PING=1",
			"HAZEL_TEST_PING_HOST="+out,
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

	// plugin→host: the plugin called the host during Initialize; the host
	// echoed the plugin ID, proving Server received it.
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read host reply: %v", err)
	}
	if got := string(b); got != "pong from host to p" {
		t.Errorf("host ping = %q, want %q", got, "pong from host to p")
	}

	// host→plugin: the Wire hook auto-dispensed the plugin's ping service, so
	// the host can call it without manual Dispense.
	pingMu.Lock()
	state := pingState["p"]
	pingMu.Unlock()
	if state == nil || state.pluginClient == nil {
		t.Fatal("Wire did not bind the plugin client")
	}
	got, err := state.pluginClient.Ping()
	if err != nil {
		t.Fatalf("plugin ping: %v", err)
	}
	if got != "pong from plugin" {
		t.Errorf("plugin ping = %q, want %q", got, "pong from plugin")
	}
}
