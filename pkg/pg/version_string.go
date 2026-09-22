package pg

import (
	"errors"
	"regexp"
	"strings"
)

// ErrNotPostgres is returned by ParseVersionString when the input does not look
// like the output of a PostgreSQL SELECT version(). Callers can use it to tell a
// malformed version string apart from a connection failure — pkg/patroni funnels
// connection errors into its failover handling, and a parse error must not be
// misread as a failover.
var ErrNotPostgres = errors.New("not a PostgreSQL version string")

// compiledBySeparator divides the version and target from the compiler and word
// size. It is a compile-time C constant in PostgreSQL, so it is not translated
// and is unaffected by lc_messages.
const compiledBySeparator = ", compiled by "

// onSeparator precedes the compilation target, where one is printed at all.
const onSeparator = " on "

// osWindows is the normalised name every Windows-flavoured OS token maps to.
const osWindows = "windows"

var (
	// The version number is the only part we require. \d+(?:\.\d+)* keeps
	// pre-release suffixes out of the capture ("18beta1" -> "18") while still
	// matching the pre-10 three-component scheme ("9.6.24").
	versionNumberRegex = regexp.MustCompile(`^PostgreSQL\s+(\d+(?:\.\d+)*)`)

	// Searched unanchored and taken at its first match: Greenplum appends
	// " compiled on <date>" and Redshift appends ", Redshift 1.0.x" after it.
	bitsRegex = regexp.MustCompile(`(\d+)-bit\b`)

	// Splits a target-triple token into its alphabetic prefix and the version
	// digits some systems append ("darwin23.2.0" -> "darwin", "gnu0.9" -> "gnu").
	osTokenRegex = regexp.MustCompile(`^([a-z_]+)[0-9.]*$`)
)

// knownOSTokens are the operating-system tokens that can appear in a target
// triple, keyed by their alphabetic prefix. Two entries are traps:
//
//   - "gnu" is both an OS (Hurd, "i686-unknown-gnu0.9") and by far the most
//     common ABI token ("x86_64-pc-linux-gnu"). Scanning left to right and
//     taking the first match resolves it, because "linux" precedes "gnu".
//     A "last token is the ABI" shortcut would break Hurd, and a "middle token"
//     heuristic would break meson's two-part targets.
//   - "w64" in "x86_64-w64-mingw32" is a vendor, not an OS; the OS is "mingw32".
var knownOSTokens = map[string]struct{}{
	"linux": {}, "darwin": {}, "freebsd": {}, "openbsd": {}, "netbsd": {},
	"dragonfly": {}, "solaris": {}, "sunos": {}, "aix": {}, "hpux": {},
	"cygwin": {}, "mingw": {}, "msys": {}, osWindows: {}, "android": {},
	"haiku": {}, "illumos": {}, "gnu": {},
}

// windowsOSTokens are the OS tokens that all mean "Windows" once normalised.
var windowsOSTokens = map[string]struct{}{
	"cygwin": {}, "mingw": {}, "msys": {}, osWindows: {},
}

// CompilationTarget is the target PostgreSQL was built for. Autoconf builds emit
// a GNU triple ("x86_64-pc-linux-gnu"), meson builds emit "cpu_family-host_system"
// ("x86_64-linux") with no vendor or ABI, and MSVC builds emit nothing at all.
// Fields are empty when version() did not carry them.
type CompilationTarget struct {
	// Raw is the target verbatim, and so the only field that is stable by
	// construction — Arch, OS, Vendor and ABI are derived from it and shift if
	// the normalisation below is ever extended.
	Raw    string
	Arch   string
	Vendor string
	OS     string
	ABI    string
}

// VersionInfo is everything SELECT version() tells us about the server.
type VersionInfo struct {
	// Raw is the full version() output, verbatim.
	Raw string
	// Version is the numeric version alone: "16.4", or "18" for "18beta1".
	Version string
	// ExtraVersion is the opaque --with-extra-version text that follows the
	// number with no separator: "beta1", "(Ubuntu 16.4-1.pgdg22.04+1)",
	// "-YB-2.20.1.0-b0". It is not always parenthesised.
	ExtraVersion string
	// Distribution is ExtraVersion unwrapped, set only when it is (...)-wrapped:
	// "Ubuntu 16.4-1.pgdg22.04+1".
	Distribution string
	Target       CompilationTarget
	// Compiler is the compiler description, which can itself contain hyphens and
	// parentheses — Aurora on Graviton reports a full triple here,
	// "aarch64-unknown-linux-gnu-gcc (GCC) 7.4.0".
	Compiler string
	// Bits is the word size as it was printed, "64" or "32", empty when absent.
	Bits string
}

// ShortVersion returns the version truncated to at most two numeric components.
// PostgreSQL has used two-component versions since 10, so this only affects 9.x
// and earlier ("9.6.24" -> "9.6"). It exists to keep PGVersion byte-identical to
// what it reported before this parser replaced its regex: pg_version is a hashed
// system-info key, and a silently changed value reads to the backend as a system
// change mid-session.
func (v VersionInfo) ShortVersion() string {
	parts := strings.Split(v.Version, ".")
	if len(parts) > 2 {
		parts = parts[:2]
	}
	return strings.Join(parts, ".")
}

// ParseVersionString parses the output of SELECT version().
//
// PostgreSQL builds that string from one of three generators, and every vendor
// string is a patch on one of them. All three have to be handled:
//
//	autoconf — PG <=17 everywhere, PG18 on most distros:
//	  PostgreSQL 16.4 (Debian 16.4-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
//	meson — PG16+ where the packager switched, and the only Windows path from 16
//	on. The target has two parts, with no vendor and no ABI:
//	  PostgreSQL 18.0 on x86_64-linux, compiled by gcc-15.1.0, 64-bit
//	MSVC — PG <=15 Windows only. There is no " on <target>" clause at all:
//	  PostgreSQL 15.4, compiled by Visual C++ build 1937, 64-bit
//
// Two things the shape does not make obvious. The text following the version
// number is arbitrary (--with-extra-version, appended with no separator), so it
// is treated as opaque and only read as a distribution when parenthesised. And
// the compiler description can itself contain hyphens and parentheses, so no
// part of it may be split on either.
//
// It returns ErrNotPostgres when the input carries no recognisable PostgreSQL
// version number — engines that impersonate PostgreSQL well enough to be pointed
// at (CockroachDB, YugabyteDB) must degrade rather than crash. Beyond that it
// never fails: every optional part is left empty when absent, and the caller
// decides how to report it.
func ParseVersionString(raw string) (VersionInfo, error) {
	info := VersionInfo{Raw: raw}

	// The absence of the separator means this is not PostgreSQL at all.
	head, tail, ok := strings.Cut(raw, compiledBySeparator)
	if !ok {
		return VersionInfo{}, ErrNotPostgres
	}

	match := versionNumberRegex.FindStringSubmatch(head)
	if match == nil {
		return VersionInfo{}, ErrNotPostgres
	}
	info.Version = match[1]

	var targetRaw string
	info.ExtraVersion, info.Distribution, targetRaw = parseHeadRemainder(head[len(match[0]):])
	info.Target = parseCompilationTarget(targetRaw)
	info.Compiler, info.Bits = parseCompilerAndBits(tail)

	// MSVC builds print no target at all, so the compiler is the only thing left
	// that identifies the platform.
	if info.Target.OS == "" && isMSVCCompiler(info.Compiler) {
		info.Target.OS = osWindows
	}

	return info, nil
}

// parseHeadRemainder splits whatever follows the version number into the extra
// version text, the distribution tag it may wrap, and the compilation target.
func parseHeadRemainder(remainder string) (extraVersion, distribution, target string) {
	// Pad with a single leading space so that " on " is found the same way
	// whether or not an extra version precedes it.
	remainder = " " + strings.TrimSpace(remainder)

	// Search for " on " past the closing parenthesis when the extra version is a
	// parenthesised tag, so that a tag containing the word "on" cannot be
	// mistaken for the target clause. Greenplum's tag is a whole sentence.
	searchFrom := 0
	if strings.HasPrefix(remainder, " (") {
		if closing := strings.Index(remainder, ")"); closing >= 0 {
			distribution = remainder[2:closing]
			searchFrom = closing + 1
		}
	}

	onIndex := strings.Index(remainder[searchFrom:], onSeparator)
	if onIndex < 0 {
		return strings.TrimSpace(remainder), distribution, ""
	}
	onIndex += searchFrom

	extraVersion = strings.TrimSpace(remainder[:onIndex])
	target = strings.TrimSpace(remainder[onIndex+len(onSeparator):])
	return extraVersion, distribution, target
}

// parseCompilerAndBits splits the ", compiled by " tail into the compiler
// description and the word size, ignoring any vendor trailer after the latter.
func parseCompilerAndBits(tail string) (compiler, bits string) {
	loc := bitsRegex.FindStringSubmatchIndex(tail)
	if loc == nil {
		return strings.TrimSpace(tail), ""
	}

	bits = tail[loc[2]:loc[3]]
	compiler = strings.TrimSpace(tail[:loc[0]])
	compiler = strings.TrimSpace(strings.TrimSuffix(compiler, ","))
	return compiler, bits
}

// parseCompilationTarget breaks a target into its parts. The first token is
// always the architecture; the first token after it that names an operating
// system is the OS, everything between them is the vendor, and everything after
// it is the ABI. When no token names an OS we keep Raw and leave the rest empty
// rather than guessing.
func parseCompilationTarget(raw string) CompilationTarget {
	target := CompilationTarget{Raw: raw}
	if raw == "" {
		return target
	}

	tokens := strings.Split(raw, "-")
	// Never strip digits from the architecture: it would turn "x86_64" into
	// "x86_" and "s390x" into "s".
	target.Arch = tokens[0]

	for i := 1; i < len(tokens); i++ {
		base, ok := osTokenBase(tokens[i])
		if !ok {
			continue
		}
		target.OS = normaliseOS(base)
		target.Vendor = strings.Join(tokens[1:i], "-")
		target.ABI = strings.Join(tokens[i+1:], "-")
		break
	}

	return target
}

// osTokenBase reports whether a target token names an operating system, and
// returns it stripped of any trailing version digits ("darwin23.2.0" -> "darwin").
func osTokenBase(token string) (string, bool) {
	match := osTokenRegex.FindStringSubmatch(strings.ToLower(token))
	if match == nil {
		return "", false
	}
	if _, known := knownOSTokens[match[1]]; !known {
		return "", false
	}
	return match[1], true
}

func normaliseOS(base string) string {
	if _, isWindows := windowsOSTokens[base]; isWindows {
		return osWindows
	}
	return base
}

func isMSVCCompiler(compiler string) bool {
	lowered := strings.ToLower(compiler)
	return strings.Contains(lowered, "visual c++") || strings.Contains(lowered, "msvc")
}
