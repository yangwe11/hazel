package hazel

import (
	"fmt"
	"strconv"
	"strings"
)

// VersionSpecifiers represents a version constraint composed of one or more
// clauses separated by commas. It implements a subset of PEP 440 version
// specifiers with the following operators:
//
//	==  version matching
//	!=  version exclusion
//	<=, >=  inclusive ordered comparison
//	<, >   exclusive ordered comparison
//	~=  compatible release (e.g., ~=1.2.0 means >= 1.2.0, < 2.0.0)
//
// Commas act as logical AND: a version must match every clause to satisfy
// the specifier.
type VersionSpecifiers string

// Validate checks that every clause in the specifier is syntactically valid.
// An empty specifier is always valid.
func (vs VersionSpecifiers) Validate() error {
	if vs == "" {
		return nil
	}
	clauses := splitSpecifier(string(vs))
	for _, clause := range clauses {
		if !isValidClause(clause) {
			return fmt.Errorf("invalid version clause %q", clause)
		}
	}
	return nil
}

// Match reports whether version satisfies the specifier.
// An empty specifier matches any version.
//
// Versions must follow semantic versioning (major.minor.patch[-prerelease]).
func Match(specifier, version string) bool {
	if specifier == "" {
		return true
	}

	ver, err := parseSemver(strings.TrimSpace(version))
	if err != nil {
		return false
	}

	clauses := splitSpecifier(specifier)
	for _, clause := range clauses {
		if !matchClause(clause, ver) {
			return false
		}
	}
	return true
}

// semver represents a parsed semantic version.
type semver struct {
	Major int
	Minor int
	Patch int
	Pre   string // pre-release suffix, e.g., "alpha.1"
	Raw   string
}

// parseSemver parses a version string such as "1.2.3" or "1.2.3-alpha.1".
// An optional leading "v" or "V" is accepted. Short forms like "1" and "1.2"
// are treated as "1.0.0" and "1.2.0" respectively.
func parseSemver(v string) (semver, error) {
	v = strings.TrimSpace(v)
	// Strip leading 'v' or 'V' if present.
	if len(v) > 0 && (v[0] == 'v' || v[0] == 'V') {
		v = v[1:]
	}

	sv := semver{Raw: v}

	// Split off pre-release suffix.
	core := v
	if idx := strings.IndexAny(v, "-+"); idx >= 0 {
		core = v[:idx]
		sv.Pre = v[idx+1:]
	}

	parts := strings.SplitN(core, ".", 3)
	if len(parts) < 1 {
		return semver{}, fmt.Errorf("invalid version: %s", v)
	}

	var err error
	sv.Major, err = strconv.Atoi(parts[0])
	if err != nil {
		return semver{}, fmt.Errorf("invalid major version in %s: %w", v, err)
	}

	if len(parts) >= 2 {
		sv.Minor, err = strconv.Atoi(parts[1])
		if err != nil {
			return semver{}, fmt.Errorf("invalid minor version in %s: %w", v, err)
		}
	}

	if len(parts) >= 3 {
		sv.Patch, err = strconv.Atoi(parts[2])
		if err != nil {
			return semver{}, fmt.Errorf("invalid patch version in %s: %w", v, err)
		}
	}

	return sv, nil
}

// compare returns -1 if s < other, 0 if s == other, or 1 if s > other.
// Pre-release versions sort before their corresponding release versions.
func (s semver) compare(other semver) int {
	if s.Major != other.Major {
		return cmp(s.Major, other.Major)
	}
	if s.Minor != other.Minor {
		return cmp(s.Minor, other.Minor)
	}
	if s.Patch != other.Patch {
		return cmp(s.Patch, other.Patch)
	}

	// Pre-release handling: a release version is greater than any
	// pre-release of the same core version.
	if s.Pre == "" && other.Pre == "" {
		return 0
	}
	if s.Pre == "" {
		return 1
	}
	if other.Pre == "" {
		return -1
	}
	// Lexicographic comparison for pre-release tags.
	if s.Pre < other.Pre {
		return -1
	}
	if s.Pre > other.Pre {
		return 1
	}
	return 0
}

func cmp(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// splitSpecifier splits the specifier by commas and returns non-empty,
// trimmed clauses.
func splitSpecifier(specifier string) []string {
	raw := strings.Split(specifier, ",")
	clauses := make([]string, 0, len(raw))
	for _, c := range raw {
		c = strings.TrimSpace(c)
		if c != "" {
			clauses = append(clauses, c)
		}
	}
	return clauses
}

// validOperators lists recognized comparison operators, ordered so that
// multi-character operators are checked before their single-character
// prefixes (e.g., "<=" before "<").
var validOperators = []string{"~=", "==", "!=", "<=", ">=", "<", ">"}

// isValidClause reports whether a single version clause is syntactically valid.
func isValidClause(clause string) bool {
	for _, op := range validOperators {
		if strings.HasPrefix(clause, op) {
			_, err := parseSemver(strings.TrimSpace(clause[len(op):]))
			return err == nil
		}
	}
	// Bare version without an operator is treated as ==.
	_, err := parseSemver(strings.TrimSpace(clause))
	return err == nil
}

// matchClause matches a single version clause against a parsed semver.
func matchClause(clause string, ver semver) bool {
	switch {
	case strings.HasPrefix(clause, "=="):
		want, err := parseSemver(strings.TrimSpace(clause[2:]))
		if err != nil {
			return false
		}
		return ver.compare(want) == 0

	case strings.HasPrefix(clause, "!="):
		want, err := parseSemver(strings.TrimSpace(clause[2:]))
		if err != nil {
			return false
		}
		return ver.compare(want) != 0

	case strings.HasPrefix(clause, ">="):
		want, err := parseSemver(strings.TrimSpace(clause[2:]))
		if err != nil {
			return false
		}
		return ver.compare(want) >= 0

	case strings.HasPrefix(clause, "<="):
		want, err := parseSemver(strings.TrimSpace(clause[2:]))
		if err != nil {
			return false
		}
		return ver.compare(want) <= 0

	case strings.HasPrefix(clause, ">"):
		want, err := parseSemver(strings.TrimSpace(clause[1:]))
		if err != nil {
			return false
		}
		return ver.compare(want) > 0

	case strings.HasPrefix(clause, "<"):
		want, err := parseSemver(strings.TrimSpace(clause[1:]))
		if err != nil {
			return false
		}
		return ver.compare(want) < 0

	case strings.HasPrefix(clause, "~="):
		// Compatible release: >= version, < next major version.
		want, err := parseSemver(strings.TrimSpace(clause[2:]))
		if err != nil {
			return false
		}
		if ver.compare(want) < 0 {
			return false
		}
		nextMajor := semver{Major: want.Major + 1}
		return ver.compare(nextMajor) < 0

	default:
		// Bare version is treated as ==.
		want, err := parseSemver(strings.TrimSpace(clause))
		if err != nil {
			return false
		}
		return ver.compare(want) == 0
	}
}
