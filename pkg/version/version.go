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
