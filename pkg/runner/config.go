package runner

import "github.com/spf13/viper"

const defaultFileCollectorOutputDir = "/var/lib/dbtune/collectors"

// fileCollectorOutputDir returns the directory where collector .ndjson files are written.
// Controlled by the viper key "file_collector.output_dir" or env var DBT_FILE_COLLECTOR_OUTPUT_DIR.
func fileCollectorOutputDir() string {
	viper.SetDefault("file_collector.output_dir", defaultFileCollectorOutputDir)
	_ = viper.BindEnv("file_collector.output_dir", "DBT_FILE_COLLECTOR_OUTPUT_DIR")
	return viper.GetString("file_collector.output_dir")
}
