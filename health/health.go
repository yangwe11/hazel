// Package health is a hazel extension that adds a plugin health capability:
// plugins report readiness and liveness to the host, and host code queries the
// latest status per plugin.
//
// Import it from both the host and plugin binaries to enable the capability:
//
//	import _ "github.com/yangwe11/hazel/health"
//
// A plugin opts in by implementing HealthAware; the host queries via StatusOf
// or Ready. The capability is wired entirely through hazel's extension registry
// (RegisterHostService) — it is not part of the hazel kernel.
package health

import (
	"net/rpc"
	"sync"
	"time"

	"github.com/yangwe11/hazel"
)

// Status is a plugin's latest reported health.
type Status struct {
	Ready bool      // last readiness report
	Live  time.Time // last heartbeat
	Msg   string    // optional message from the last report
}

var (
	mu     sync.RWMutex
	status = map[string]Status{}
)

// StatusOf returns the latest health of a plugin and whether it has ever
// reported.
func StatusOf(pluginID string) (Status, bool) {
	mu.RLock()
	defer mu.RUnlock()
	s, ok := status[pluginID]
	return s, ok
}

// Ready reports whether a plugin has reported ready.
func Ready(pluginID string) bool {
	s, ok := StatusOf(pluginID)
	return ok && s.Ready
}

// Reporter is the plugin-side client for reporting health to the host.
type Reporter struct {
	client *rpc.Client
}

// Ready reports the plugin as ready.
func (r *Reporter) Ready(msg string) error { return r.report(true, msg) }

// NotReady reports the plugin as not ready.
func (r *Reporter) NotReady(msg string) error { return r.report(false, msg) }

// Live sends a heartbeat.
func (r *Reporter) Live() error {
	return r.client.Call("Plugin.Live", hazel.Empty{}, &hazel.Empty{})
}

func (r *Reporter) report(ready bool, msg string) error {
	return r.client.Call("Plugin.Report", ReportArgs{Ready: ready, Msg: msg}, &hazel.Empty{})
}

// ReportArgs is the wire payload for a readiness report. It must be exported so
// net/rpc exposes the Report method.
type ReportArgs struct {
	Ready bool
	Msg   string
}

// HealthAware lets a plugin receive a Reporter during Initialize.
type HealthAware interface {
	SetReporter(*Reporter)
}

// server serves the host side of the health service; one per plugin.
type server struct {
	pluginID string
}

func (s *server) Report(args ReportArgs, _ *hazel.Empty) error {
	mu.Lock()
	st := status[s.pluginID]
	st.Ready = args.Ready
	st.Msg = args.Msg
	status[s.pluginID] = st
	mu.Unlock()
	return nil
}

func (s *server) Live(_ hazel.Empty, _ *hazel.Empty) error {
	mu.Lock()
	st := status[s.pluginID]
	st.Live = time.Now()
	status[s.pluginID] = st
	mu.Unlock()
	return nil
}

func init() {
	hazel.RegisterHostService(hazel.HostService{
		Name:   "health",
		Server: func(_ *hazel.Manager, pluginID string) any { return &server{pluginID: pluginID} },
		Client: func(client *rpc.Client) any { return &Reporter{client: client} },
		Inject: func(impl, client any) {
			if h, ok := impl.(HealthAware); ok {
				h.SetReporter(client.(*Reporter))
			}
		},
	})
}
