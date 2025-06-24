package version

import (
	"fmt"
	"runtime"
)

// These variables will be injected by goreleaser via ldflags
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// GetVersion returns the complete version information as a string
func GetVersion() string {
	return fmt.Sprintf("dbtune-agent %s (commit: %s, built: %s, go: %s)",
		Version, Commit, Date, runtime.Version())
}

// GetVersionOnly returns just the version string
func GetVersionOnly() string {
	return Version
}

// GetCommit returns the commit hash
func GetCommit() string {
	return Commit
}

// GetBuildDate returns the build date
func GetBuildDate() string {
	return Date
}

// GetGoVersion returns the Go runtime version
func GetGoVersion() string {
	return runtime.Version()
}
