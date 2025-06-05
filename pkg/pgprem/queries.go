package pgprem

import (
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaypipes/ghw"
)

func GetDiskType(pgDriver *pgxpool.Pool) (string, error) {
	// First we query PostgreSQL to get data directory mount point
	dataDir, err := pg.DataDirectory(pgDriver)
	if err != nil {
		return "UNKNOWN", err
	}

	// Resolve symlinks and get absolute path
	realPath, err := filepath.EvalSymlinks(dataDir)
	if err != nil {
		return "UNKNOWN", err
	}

	// Get device name using df
	cmd := exec.Command("df", realPath)
	output, err := cmd.Output()
	if err != nil {
		return "UNKNOWN", err
	}

	// Parse df output - skip header line and get first field
	var deviceName string
	lines := strings.Split(string(output), "\n")
	if len(lines) >= 2 {
		fields := strings.Fields(lines[1])
		if len(fields) > 0 {
			deviceName = fields[0]
		}
	}

	// Get block storage information using ghw
	block, err := ghw.Block()
	if err != nil {
		return "UNKNOWN", err
	}

	// Find the disk type for the device
	for _, disk := range block.Disks {
		// Check if this disk matches our device
		possiblePaths := []string{
			deviceName,
			"/dev/" + filepath.Base(deviceName),
			disk.Name,
			"/dev/" + disk.Name,
		}

		for _, p := range possiblePaths {
			if p == deviceName {
				// Check for NVMe drives
				if disk.StorageController == ghw.STORAGE_CONTROLLER_NVME {
					return "NVME", nil
				}
				// Check for SSDs vs HDDs
				if disk.DriveType == ghw.DRIVE_TYPE_HDD {
					return "HDD", nil
				}
				return "SSD", nil
			}
		}
	}

	return "UNKNOWN", nil
}
