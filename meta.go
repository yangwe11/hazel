package hazel

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	// PluginTypeStatic identifies a static plugin that is not launched by the engine.
	PluginTypeStatic = "STATIC"
	// PluginTypeNative identifies a native plugin. Go is currently supported.
	PluginTypeNative = "NATIVE"
	// PluginTypeBridge identifies a bridge plugin that wraps an external service (e.g., Redis, Kafka).
	PluginTypeBridge = "BRIDGE"
)

// PluginMeta holds the metadata for a plugin.
// Each plugin must provide a plugin.yaml file in its root directory for discovery.
type PluginMeta struct {
	// Required fields
	ID      string `yaml:"id"`      // Unique identifier for the plugin.
	Version string `yaml:"version"` // Version must follow semantic versioning (e.g., 1.2.3).
	Name    string `yaml:"name"`    // Human-readable display name.
	Type    string `yaml:"type"`    // Plugin type: NATIVE, STATIC, or BRIDGE.

	// Optional fields
	CmdName           string            `yaml:"cmdName"`           // Executable name; defaults to ID if empty.
	Description       string            `yaml:"description"`       // Short description of the plugin.
	Author            string            `yaml:"author"`            // Plugin author.
	Category          string            `yaml:"category"`          // Grouping category.
	Tags              []string          `yaml:"tags"`              // Search tags.
	EngineRequirement VersionSpecifiers `yaml:"engineRequirement"` // Engine version constraint.
	RequiredAuthGroup string            `yaml:"requiredAuthGroup"` // Auth group required to use this plugin.
	Depends           []Depend          `yaml:"depends"`           // Dependencies on other plugins.
	Extensions        any               `yaml:"extensions"`        // Plugin-specific extension data.

	// Runtime fields (not serialized)
	pluginDir string
}

// Depend defines a dependency on another plugin.
type Depend struct {
	ID          string            `yaml:"id"`          // ID of the required plugin.
	Requirement VersionSpecifiers `yaml:"requirement"` // Version constraint for the dependency.
	Optional    bool              `yaml:"optional"`    // If true, the dependency is optional.
}

const pluginMetaFile = "plugin.yaml"

// LoadMeta reads and parses the plugin.yaml file from the given directory.
// It validates that all required fields are present and well-formed.
func LoadMeta(pluginDir string) (PluginMeta, error) {
	pluginDir = filepath.Clean(pluginDir)
	metaPath := filepath.Join(pluginDir, pluginMetaFile)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return PluginMeta{}, fmt.Errorf("cannot read %s: %w", metaPath, err)
	}
	meta := PluginMeta{
		pluginDir: pluginDir,
	}
	if err := yaml.Unmarshal(data, &meta); err != nil {
		return PluginMeta{}, fmt.Errorf("cannot parse %s: %w", metaPath, err)
	}

	if err := validateMeta(meta); err != nil {
		return PluginMeta{}, fmt.Errorf("%s: %w", metaPath, err)
	}

	return meta, nil
}

// ScanDirectory walks the given directory looking for immediate
// subdirectories that contain a plugin.yaml file. It returns metadata
// for every discovered plugin.
//
// ScanDirectory does NOT recurse; it only checks one level deep.
func ScanDirectory(root string) ([]PluginMeta, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("cannot scan plugin directory %s: %w", root, err)
	}

	var metas []PluginMeta
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		pluginDir := filepath.Join(root, entry.Name())
		meta, err := LoadMeta(pluginDir)
		if err != nil {
			return nil, err
		}
		metas = append(metas, meta)
	}

	return metas, nil
}

// checkEngineCompatibility reports whether the plugin's engineRequirement is
// satisfied by the running engine version. An empty requirement always passes.
func checkEngineCompatibility(meta PluginMeta) error {
	if Match(string(meta.EngineRequirement), Version) {
		return nil
	}
	return fmt.Errorf("%w: plugin %s requires engine %q, but hazel is %s",
		ErrEngineMismatch, meta.ID, string(meta.EngineRequirement), Version)
}

// validateMeta checks that all required fields are present and valid,
// and that optional fields are well-formed when provided.
func validateMeta(meta PluginMeta) error {
	// --- Required fields ---

	if meta.ID == "" {
		return fmt.Errorf("plugin ID is required")
	}
	if meta.Name == "" {
		return fmt.Errorf("plugin name is required")
	}
	if meta.Version == "" {
		return fmt.Errorf("plugin version is required")
	}
	if _, err := parseSemver(meta.Version); err != nil {
		return fmt.Errorf("invalid plugin version %q: must follow semantic versioning (e.g., 1.2.3)", meta.Version)
	}

	switch meta.Type {
	case PluginTypeNative, PluginTypeStatic, PluginTypeBridge:
		// valid
	case "":
		return fmt.Errorf("plugin type is required (must be NATIVE, STATIC, or BRIDGE)")
	default:
		return fmt.Errorf("unknown plugin type %q: must be NATIVE, STATIC, or BRIDGE", meta.Type)
	}

	// --- Optional fields ---

	if meta.CmdName != "" {
		if strings.ContainsAny(meta.CmdName, "/\\") {
			return fmt.Errorf("cmdName must be a plain filename, got %q", meta.CmdName)
		}
	}

	if err := meta.EngineRequirement.Validate(); err != nil {
		return fmt.Errorf("engineRequirement: %w", err)
	}

	depIDs := make(map[string]int)
	for i, dep := range meta.Depends {
		if dep.ID == "" {
			return fmt.Errorf("depends[%d]: ID is required", i)
		}
		if dep.ID == meta.ID {
			return fmt.Errorf("depends[%d]: plugin cannot depend on itself (%q)", i, dep.ID)
		}
		if first, exists := depIDs[dep.ID]; exists {
			return fmt.Errorf("depends[%d]: duplicate dependency %q (already declared at index %d)", i, dep.ID, first)
		}
		depIDs[dep.ID] = i
		if err := dep.Requirement.Validate(); err != nil {
			return fmt.Errorf("depends[%d] (%s): %w", i, dep.ID, err)
		}
	}

	return nil
}
