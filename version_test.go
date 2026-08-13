package hazel

import "testing"

func TestMatch(t *testing.T) {
	tests := []struct {
		spec, version string
		want          bool
	}{
		{"", "1.2.3", true},
		{"==1.2.3", "1.2.3", true},
		{"==1.2.3", "1.2.4", false},
		{"1.2.3", "1.2.3", true}, // bare version is treated as ==
		{"!=1.2.3", "1.2.4", true},
		{"!=1.2.3", "1.2.3", false},
		{">=1.2.0", "1.2.0", true},
		{">=1.2.0", "1.1.9", false},
		{">1.2.0", "1.2.1", true},
		{">1.2.0", "1.2.0", false},
		{"<1.2.0", "1.1.9", true},
		{"<1.2.0", "1.2.0", false},
		{"<=1.2.0", "1.2.0", true},
		{"~=1.2.0", "1.5.0", true},
		{"~=1.2.0", "2.0.0", false},
		{">=1.0.0, <2.0.0", "1.5.0", true},
		{">=1.0.0, <2.0.0", "2.5.0", false},
		{"v1.2.3", "1.2.3", true},
		{"==1.2.3", "v1.2.3", true},
		{"not-a-version", "1.2.3", false},
	}
	for _, tt := range tests {
		if got := Match(tt.spec, tt.version); got != tt.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tt.spec, tt.version, got, tt.want)
		}
	}
}

func TestParseSemver(t *testing.T) {
	tests := []struct {
		in                  string
		major, minor, patch int
		pre                 string
		err                 bool
	}{
		{"1.2.3", 1, 2, 3, "", false},
		{"v1.2.3", 1, 2, 3, "", false},
		{"1", 1, 0, 0, "", false},
		{"1.2", 1, 2, 0, "", false},
		{"1.2.3-alpha.1", 1, 2, 3, "alpha.1", false},
		{"1.2.3+build.7", 1, 2, 3, "build.7", false},
		{"", 0, 0, 0, "", true},
		{"abc", 0, 0, 0, "", true},
		{"1.x.3", 0, 0, 0, "", true},
	}
	for _, tt := range tests {
		got, err := parseSemver(tt.in)
		if tt.err {
			if err == nil {
				t.Errorf("parseSemver(%q) expected error", tt.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSemver(%q) unexpected error: %v", tt.in, err)
			continue
		}
		if got.Major != tt.major || got.Minor != tt.minor || got.Patch != tt.patch || got.Pre != tt.pre {
			t.Errorf("parseSemver(%q) = %+v, want (%d, %d, %d, %q)",
				tt.in, got, tt.major, tt.minor, tt.patch, tt.pre)
		}
	}
}

func TestVersionSpecifiersValidate(t *testing.T) {
	valid := []VersionSpecifiers{"", "==1.2.3", ">=1.0.0, <2.0.0", "~=1.2.0", "!=1.2.0", "1.2.3"}
	for _, vs := range valid {
		if err := vs.Validate(); err != nil {
			t.Errorf("VersionSpecifiers(%q).Validate() = %v, want nil", vs, err)
		}
	}

	invalid := []VersionSpecifiers{"=>1.2.3", "==abc", "1.2.3.4.5"}
	for _, vs := range invalid {
		if err := vs.Validate(); err == nil {
			t.Errorf("VersionSpecifiers(%q).Validate() = nil, want error", vs)
		}
	}
}
