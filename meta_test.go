package hazel

import (
	"strings"
	"testing"
)

func TestValidateMeta(t *testing.T) {
	base := func() PluginMeta {
		return PluginMeta{ID: "p", Name: "Plugin", Version: "1.0.0", Type: PluginTypeNative}
	}

	if err := validateMeta(base()); err != nil {
		t.Fatalf("valid meta should pass: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(*PluginMeta)
		wantErr string
	}{
		{"missing id", func(m *PluginMeta) { m.ID = "" }, "ID is required"},
		{"missing name", func(m *PluginMeta) { m.Name = "" }, "name is required"},
		{"missing version", func(m *PluginMeta) { m.Version = "" }, "version is required"},
		{"bad version", func(m *PluginMeta) { m.Version = "abc" }, "semantic versioning"},
		{"missing type", func(m *PluginMeta) { m.Type = "" }, "type is required"},
		{"bad type", func(m *PluginMeta) { m.Type = "WASM" }, "unknown plugin type"},
		{"bad cmdName", func(m *PluginMeta) { m.CmdName = "a/b" }, "plain filename"},
		{"self dependency", func(m *PluginMeta) { m.Depends = []Depend{{ID: "p"}} }, "itself"},
		{"duplicate dependency", func(m *PluginMeta) { m.Depends = []Depend{{ID: "x"}, {ID: "x"}} }, "duplicate"},
		{"missing dependency id", func(m *PluginMeta) { m.Depends = []Depend{{}} }, "ID is required"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := base()
			c.mutate(&m)
			err := validateMeta(m)
			if err == nil {
				t.Fatalf("expected error")
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), c.wantErr)
			}
		})
	}
}
