package pg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The strings below are recorded from real servers rather than constructed, one
// per generator (autoconf, meson, MSVC) crossed with the platforms we actually
// see. The PG 12-18 container matrix cannot stand in for this: every image in it
// is postgres:<n>-alpine, so all seven emit the same autoconf/musl shape.
func TestParseVersionString(t *testing.T) {
	tests := []struct {
		name string
		raw  string

		version      string
		extraVersion string
		distribution string
		targetRaw    string
		arch         string
		vendor       string
		os           string
		abi          string
		compiler     string
		bits         string
	}{
		// --- autoconf ---
		{
			name:         "debian pgdg",
			raw:          "PostgreSQL 16.4 (Debian 16.4-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit",
			version:      "16.4",
			extraVersion: "(Debian 16.4-1.pgdg120+1)",
			distribution: "Debian 16.4-1.pgdg120+1",
			targetRaw:    "x86_64-pc-linux-gnu",
			arch:         "x86_64",
			vendor:       "pc",
			os:           "linux",
			abi:          "gnu",
			compiler:     "gcc (Debian 12.2.0-14) 12.2.0",
			bits:         "64",
		},
		{
			name:         "ubuntu pgdg",
			raw:          "PostgreSQL 16.4 (Ubuntu 16.4-1.pgdg22.04+1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0, 64-bit",
			version:      "16.4",
			extraVersion: "(Ubuntu 16.4-1.pgdg22.04+1)",
			distribution: "Ubuntu 16.4-1.pgdg22.04+1",
			targetRaw:    "x86_64-pc-linux-gnu",
			arch:         "x86_64",
			vendor:       "pc",
			os:           "linux",
			abi:          "gnu",
			compiler:     "gcc (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0",
			bits:         "64",
		},
		{
			// The compiler here carries both parentheses and hyphens; neither
			// may leak into the target or the bits.
			name:      "rhel family",
			raw:       "PostgreSQL 16.4 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 11.4.1 20230605 (Red Hat 11.4.1-2), 64-bit",
			version:   "16.4",
			targetRaw: "x86_64-pc-linux-gnu",
			arch:      "x86_64",
			vendor:    "pc",
			os:        "linux",
			abi:       "gnu",
			compiler:  "gcc (GCC) 11.4.1 20230605 (Red Hat 11.4.1-2)",
			bits:      "64",
		},
		{
			// Recorded from postgres:12-alpine, the oldest image in the
			// integration matrix. Note the vendor is "unknown", not "pc".
			name:      "alpine musl pg12",
			raw:       "PostgreSQL 12.22 on aarch64-unknown-linux-musl, compiled by gcc (Alpine 14.2.0) 14.2.0, 64-bit",
			version:   "12.22",
			targetRaw: "aarch64-unknown-linux-musl",
			arch:      "aarch64",
			vendor:    "unknown",
			os:        "linux",
			abi:       "musl",
			compiler:  "gcc (Alpine 14.2.0) 14.2.0",
			bits:      "64",
		},
		{
			// Recorded from postgres:18-alpine, the newest image in the matrix.
			name:      "alpine musl pg18",
			raw:       "PostgreSQL 18.6 on aarch64-unknown-linux-musl, compiled by gcc (Alpine 15.2.0) 15.2.0, 64-bit",
			version:   "18.6",
			targetRaw: "aarch64-unknown-linux-musl",
			arch:      "aarch64",
			vendor:    "unknown",
			os:        "linux",
			abi:       "musl",
			compiler:  "gcc (Alpine 15.2.0) 15.2.0",
			bits:      "64",
		},
		{
			// Three-part target whose OS token carries a dotted version.
			name:         "macos homebrew arm64",
			raw:          "PostgreSQL 16.2 (Homebrew) on aarch64-apple-darwin23.2.0, compiled by Apple clang version 15.0.0 (clang-1500.1.0.2.5), 64-bit",
			version:      "16.2",
			extraVersion: "(Homebrew)",
			distribution: "Homebrew",
			targetRaw:    "aarch64-apple-darwin23.2.0",
			arch:         "aarch64",
			vendor:       "apple",
			os:           "darwin",
			compiler:     "Apple clang version 15.0.0 (clang-1500.1.0.2.5)",
			bits:         "64",
		},

		// --- meson: two-part target, no vendor and no ABI ---
		{
			name:      "meson linux",
			raw:       "PostgreSQL 18.0 on x86_64-linux, compiled by gcc-15.1.0, 64-bit",
			version:   "18.0",
			targetRaw: "x86_64-linux",
			arch:      "x86_64",
			os:        "linux",
			compiler:  "gcc-15.1.0",
			bits:      "64",
		},
		{
			name:      "meson macos",
			raw:       "PostgreSQL 17.2 on aarch64-darwin, compiled by clang-16.0.0, 64-bit",
			version:   "17.2",
			targetRaw: "aarch64-darwin",
			arch:      "aarch64",
			os:        "darwin",
			compiler:  "clang-16.0.0",
			bits:      "64",
		},
		{
			// Windows from PG 16 on does have an " on <target>" clause, so the
			// MSVC compiler sniff must not be the only way we reach "windows".
			name:      "meson windows",
			raw:       "PostgreSQL 17.2 on x86_64-windows, compiled by msvc-19.40.33811, 64-bit",
			version:   "17.2",
			targetRaw: "x86_64-windows",
			arch:      "x86_64",
			os:        "windows",
			compiler:  "msvc-19.40.33811",
			bits:      "64",
		},

		// --- MSVC: no " on <target>" clause at all ---
		{
			name:     "msvc windows",
			raw:      "PostgreSQL 15.4, compiled by Visual C++ build 1937, 64-bit",
			version:  "15.4",
			os:       "windows",
			compiler: "Visual C++ build 1937",
			bits:     "64",
		},
		{
			name:     "msvc windows 32 bit",
			raw:      "PostgreSQL 9.6.24, compiled by Visual C++ build 1800, 32-bit",
			version:  "9.6.24",
			os:       "windows",
			compiler: "Visual C++ build 1800",
			bits:     "32",
		},

		// --- managed services ---
		{
			name:      "aws rds x86",
			raw:       "PostgreSQL 16.3 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 7.3.1 20180712 (Red Hat 7.3.1-12), 64-bit",
			version:   "16.3",
			targetRaw: "x86_64-pc-linux-gnu",
			arch:      "x86_64",
			vendor:    "pc",
			os:        "linux",
			abi:       "gnu",
			compiler:  "gcc (GCC) 7.3.1 20180712 (Red Hat 7.3.1-12)",
			bits:      "64",
		},
		{
			// The compiler name is itself a full target triple. Nothing may fall
			// back to splitting the tail on hyphens.
			name:      "aurora graviton",
			raw:       "PostgreSQL 12.7 on aarch64-unknown-linux-gnu, compiled by aarch64-unknown-linux-gnu-gcc (GCC) 7.4.0, 64-bit",
			version:   "12.7",
			targetRaw: "aarch64-unknown-linux-gnu",
			arch:      "aarch64",
			vendor:    "unknown",
			os:        "linux",
			abi:       "gnu",
			compiler:  "aarch64-unknown-linux-gnu-gcc (GCC) 7.4.0",
			bits:      "64",
		},
		{
			name:      "azure flexible server",
			raw:       "PostgreSQL 16.4 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 11.4.1 20230605 (Red Hat 11.4.1-2), 64-bit",
			version:   "16.4",
			targetRaw: "x86_64-pc-linux-gnu",
			arch:      "x86_64",
			vendor:    "pc",
			os:        "linux",
			abi:       "gnu",
			compiler:  "gcc (GCC) 11.4.1 20230605 (Red Hat 11.4.1-2)",
			bits:      "64",
		},
		{
			name:      "google cloud sql",
			raw:       "PostgreSQL 15.6 on x86_64-pc-linux-gnu, compiled by Debian clang version 12.0.1, 64-bit",
			version:   "15.6",
			targetRaw: "x86_64-pc-linux-gnu",
			arch:      "x86_64",
			vendor:    "pc",
			os:        "linux",
			abi:       "gnu",
			compiler:  "Debian clang version 12.0.1",
			bits:      "64",
		},
		{
			name:         "supabase",
			raw:          "PostgreSQL 15.1 (Ubuntu 15.1-1.pgdg20.04+1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 9.4.0-1ubuntu1~20.04.1) 9.4.0, 64-bit",
			version:      "15.1",
			extraVersion: "(Ubuntu 15.1-1.pgdg20.04+1)",
			distribution: "Ubuntu 15.1-1.pgdg20.04+1",
			targetRaw:    "x86_64-pc-linux-gnu",
			arch:         "x86_64",
			vendor:       "pc",
			os:           "linux",
			abi:          "gnu",
			compiler:     "gcc (Ubuntu 9.4.0-1ubuntu1~20.04.1) 9.4.0",
			bits:         "64",
		},

		// --- extra-version text, which is not always parenthesised ---
		{
			name:         "beta release",
			raw:          "PostgreSQL 18beta1 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 14.2.1, 64-bit",
			version:      "18",
			extraVersion: "beta1",
			targetRaw:    "x86_64-pc-linux-gnu",
			arch:         "x86_64",
			vendor:       "pc",
			os:           "linux",
			abi:          "gnu",
			compiler:     "gcc (GCC) 14.2.1",
			bits:         "64",
		},
		{
			name:         "release candidate",
			raw:          "PostgreSQL 17rc1 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 14.2.1, 64-bit",
			version:      "17",
			extraVersion: "rc1",
			targetRaw:    "x86_64-pc-linux-gnu",
			arch:         "x86_64",
			vendor:       "pc",
			os:           "linux",
			abi:          "gnu",
			compiler:     "gcc (GCC) 14.2.1",
			bits:         "64",
		},
		{
			// The extra version contains hyphens and digits, so the search for
			// " on " has to be unanchored.
			name:         "yugabytedb",
			raw:          "PostgreSQL 11.2-YB-2.20.1.0-b0 on x86_64-pc-linux-gnu, compiled by clang version 15.0.3, 64-bit",
			version:      "11.2",
			extraVersion: "-YB-2.20.1.0-b0",
			targetRaw:    "x86_64-pc-linux-gnu",
			arch:         "x86_64",
			vendor:       "pc",
			os:           "linux",
			abi:          "gnu",
			compiler:     "clang version 15.0.3",
			bits:         "64",
		},

		// --- vendor text past the parts we care about ---
		{
			// Greenplum's tag is a whole sentence and it appends " compiled on
			// <date>" after the word size.
			name:         "greenplum trailer",
			raw:          "PostgreSQL 9.4.24 (Greenplum Database 6.21.0 build commit:abc123) on x86_64-unknown-linux-gnu, compiled by gcc (GCC) 6.4.0, 64-bit compiled on Aug 10 2022 12:00:00",
			version:      "9.4.24",
			extraVersion: "(Greenplum Database 6.21.0 build commit:abc123)",
			distribution: "Greenplum Database 6.21.0 build commit:abc123",
			targetRaw:    "x86_64-unknown-linux-gnu",
			arch:         "x86_64",
			vendor:       "unknown",
			os:           "linux",
			abi:          "gnu",
			compiler:     "gcc (GCC) 6.4.0",
			bits:         "64",
		},
		{
			// Redshift prints no word size at all.
			name:      "redshift no bits",
			raw:       "PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.68205",
			version:   "8.0.2",
			targetRaw: "i686-pc-linux-gnu",
			arch:      "i686",
			vendor:    "pc",
			os:        "linux",
			abi:       "gnu",
			compiler:  "GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.68205",
			bits:      "",
		},

		// --- target shapes with awkward tokens ---
		{
			// "w64" is a vendor, not an OS; "mingw32" is the OS even on 64-bit.
			name:      "mingw",
			raw:       "PostgreSQL 14.5 on x86_64-w64-mingw32, compiled by gcc.exe (GCC) 12.2.0, 64-bit",
			version:   "14.5",
			targetRaw: "x86_64-w64-mingw32",
			arch:      "x86_64",
			vendor:    "w64",
			os:        "windows",
			compiler:  "gcc.exe (GCC) 12.2.0",
			bits:      "64",
		},
		{
			// "gnu" is an OS here rather than an ABI. Taking the first known OS
			// token from the left is what tells the two uses apart.
			name:      "hurd",
			raw:       "PostgreSQL 15.3 on i686-unknown-gnu0.9, compiled by gcc (Debian 12.2.0-14) 12.2.0, 32-bit",
			version:   "15.3",
			targetRaw: "i686-unknown-gnu0.9",
			arch:      "i686",
			vendor:    "unknown",
			os:        "gnu",
			compiler:  "gcc (Debian 12.2.0-14) 12.2.0",
			bits:      "32",
		},
		{
			name:      "freebsd",
			raw:       "PostgreSQL 16.1 on amd64-portbld-freebsd14.0, compiled by FreeBSD clang version 16.0.6, 64-bit",
			version:   "16.1",
			targetRaw: "amd64-portbld-freebsd14.0",
			arch:      "amd64",
			vendor:    "portbld",
			os:        "freebsd",
			compiler:  "FreeBSD clang version 16.0.6",
			bits:      "64",
		},
		{
			name:      "solaris",
			raw:       "PostgreSQL 13.9 on sparc-sun-solaris2.11, compiled by gcc (GCC) 10.3.0, 64-bit",
			version:   "13.9",
			targetRaw: "sparc-sun-solaris2.11",
			arch:      "sparc",
			vendor:    "sun",
			os:        "solaris",
			compiler:  "gcc (GCC) 10.3.0",
			bits:      "64",
		},
		{
			// s390x must survive the OS-token digit strip untouched.
			name:      "s390x",
			raw:       "PostgreSQL 16.4 on s390x-ibm-linux-gnu, compiled by gcc (GCC) 11.4.1, 64-bit",
			version:   "16.4",
			targetRaw: "s390x-ibm-linux-gnu",
			arch:      "s390x",
			vendor:    "ibm",
			os:        "linux",
			abi:       "gnu",
			compiler:  "gcc (GCC) 11.4.1",
			bits:      "64",
		},
		{
			// No token names an OS: keep the raw target, guess nothing.
			name:      "unrecognised target",
			raw:       "PostgreSQL 16.4 on riscv64-buildroot-someos, compiled by gcc (GCC) 13.1.0, 64-bit",
			version:   "16.4",
			targetRaw: "riscv64-buildroot-someos",
			arch:      "riscv64",
			compiler:  "gcc (GCC) 13.1.0",
			bits:      "64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseVersionString(tt.raw)
			require.NoError(t, err)

			assert.Equal(t, tt.raw, got.Raw, "Raw")
			assert.Equal(t, tt.version, got.Version, "Version")
			assert.Equal(t, tt.extraVersion, got.ExtraVersion, "ExtraVersion")
			assert.Equal(t, tt.distribution, got.Distribution, "Distribution")
			assert.Equal(t, tt.targetRaw, got.Target.Raw, "Target.Raw")
			assert.Equal(t, tt.arch, got.Target.Arch, "Target.Arch")
			assert.Equal(t, tt.vendor, got.Target.Vendor, "Target.Vendor")
			assert.Equal(t, tt.os, got.Target.OS, "Target.OS")
			assert.Equal(t, tt.abi, got.Target.ABI, "Target.ABI")
			assert.Equal(t, tt.compiler, got.Compiler, "Compiler")
			assert.Equal(t, tt.bits, got.Bits, "Bits")
		})
	}
}

// Anything that is not recognisably PostgreSQL must come back as an error rather
// than a panic. Before this parser, PGVersion indexed the regex submatch without
// checking it matched, so every one of these crashed the adapter it ran in.
func TestParseVersionString_NotPostgres(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"garbage", "garbage"},
		{"cockroachdb", "CockroachDB CCL v23.1.11 (x86_64-pc-linux-gnu, built 2023/11/13 16:39:00, go1.19.13)"},
		{"no version number", "PostgreSQL on x86_64-pc-linux-gnu, compiled by gcc, 64-bit"},
		{"other engine", "MySQL 8.0.35, compiled by gcc, 64-bit"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				got, err := ParseVersionString(tt.raw)
				assert.ErrorIs(t, err, ErrNotPostgres)
				assert.Equal(t, VersionInfo{}, got)
			})
		})
	}
}

// ShortVersion is what PGVersion reports, and pg_version is a hashed system-info
// key. It has to stay byte-identical to what the old `PostgreSQL (\d+\.\d+)`
// regex produced, or upgrading the agent looks to the backend like the system
// changed underneath a running tuning session.
func TestVersionInfo_ShortVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{"two components unchanged", "16.4", "16.4"},
		{"three components truncated", "9.6.24", "9.6"},
		{"four components truncated", "8.0.2.1", "8.0"},
		{"single component", "18", "18"},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, VersionInfo{Version: tt.version}.ShortVersion())
		})
	}
}

// The major version drives collector gating, so check it survives the round trip
// from a raw version() string through the parser and the truncation.
func TestParseVersionString_FeedsMajorVersion(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
	}{
		{"pg12", "PostgreSQL 12.22 on aarch64-unknown-linux-musl, compiled by gcc (Alpine 14.2.0) 14.2.0, 64-bit", 12},
		{"pg17", "PostgreSQL 17.11 on aarch64-unknown-linux-musl, compiled by gcc (Alpine 15.2.0) 15.2.0, 64-bit", 17},
		{"pg18 meson", "PostgreSQL 18.0 on x86_64-linux, compiled by gcc-15.1.0, 64-bit", 18},
		{"beta", "PostgreSQL 18beta1 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 14.2.1, 64-bit", 18},
		{"pre-10 scheme", "PostgreSQL 9.6.24, compiled by Visual C++ build 1800, 32-bit", 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := ParseVersionString(tt.raw)
			require.NoError(t, err)

			major, err := PGMajorVersion(info.ShortVersion())
			require.NoError(t, err)
			assert.Equal(t, tt.want, major)
		})
	}
}
