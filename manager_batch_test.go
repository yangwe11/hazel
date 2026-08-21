package hazel

import (
	"errors"
	"testing"
)

func TestResolveOrder(t *testing.T) {
	// a → b → c, and a → c.
	metas := map[string]PluginMeta{
		"a": {ID: "a", Depends: []Depend{{ID: "b"}, {ID: "c"}}},
		"b": {ID: "b", Depends: []Depend{{ID: "c"}}},
		"c": {ID: "c"},
	}

	batches, err := buildGraph(metas).resolveOrder()
	if err != nil {
		t.Fatalf("resolveOrder: %v", err)
	}

	want := [][]string{{"c"}, {"b"}, {"a"}}
	if len(batches) != len(want) {
		t.Fatalf("got %d batches %v, want %v", len(batches), batches, want)
	}
	for i := range want {
		if len(batches[i]) != len(want[i]) || batches[i][0] != want[i][0] {
			t.Errorf("batch %d = %v, want %v", i, batches[i], want[i])
		}
	}
}

func TestResolveOrderIndependentBatch(t *testing.T) {
	// b and c are independent and must share a batch.
	metas := map[string]PluginMeta{
		"a": {ID: "a", Depends: []Depend{{ID: "b"}, {ID: "c"}}},
		"b": {ID: "b"},
		"c": {ID: "c"},
	}

	batches, err := buildGraph(metas).resolveOrder()
	if err != nil {
		t.Fatalf("resolveOrder: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("got %d batches %v, want 2", len(batches), batches)
	}
	if len(batches[0]) != 2 {
		t.Errorf("first batch = %v, want both b and c", batches[0])
	}
}

func TestResolveOrderCycle(t *testing.T) {
	metas := map[string]PluginMeta{
		"a": {ID: "a", Depends: []Depend{{ID: "b"}}},
		"b": {ID: "b", Depends: []Depend{{ID: "a"}}},
	}
	if _, err := buildGraph(metas).resolveOrder(); !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle, got %v", err)
	}
}

func TestValidateDependencyVersions(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		metas := map[string]PluginMeta{
			"a": {ID: "a", Depends: []Depend{{ID: "missing"}}},
		}
		if err := validateDependencyVersions(metas); !errors.Is(err, ErrDependencyMissing) {
			t.Fatalf("expected ErrDependencyMissing, got %v", err)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		metas := map[string]PluginMeta{
			"a": {ID: "a", Depends: []Depend{{ID: "b", Requirement: ">=2.0.0"}}},
			"b": {ID: "b", Version: "1.0.0"},
		}
		if err := validateDependencyVersions(metas); !errors.Is(err, ErrVersionMismatch) {
			t.Fatalf("expected ErrVersionMismatch, got %v", err)
		}
	})

	t.Run("optional missing ok", func(t *testing.T) {
		metas := map[string]PluginMeta{
			"a": {ID: "a", Depends: []Depend{{ID: "missing", Optional: true}}},
		}
		if err := validateDependencyVersions(metas); err != nil {
			t.Fatalf("optional missing dependency should pass, got %v", err)
		}
	})

	t.Run("satisfied", func(t *testing.T) {
		metas := map[string]PluginMeta{
			"a": {ID: "a", Depends: []Depend{{ID: "b", Requirement: ">=1.0.0"}}},
			"b": {ID: "b", Version: "1.5.0"},
		}
		if err := validateDependencyVersions(metas); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
