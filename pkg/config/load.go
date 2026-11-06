// Package config loads the agent's configuration: the dbtune.yaml file plus
// DBT_* environment variables, and the settings that several packages share.
// A setting read by only one package (an adapter's section, the collector
// overrides, the backend credentials) is parsed next to the code that uses it.
package config

import (
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Load points the global viper at dbtune.yaml and the DBT_ environment
// variables. A missing config file is not an error: the agent can run on
// environment variables alone.
func Load() {
	// Set the file name of the configurations file
	viper.SetConfigName("dbtune")
	viper.SetConfigType("yaml")

	// Locations where to look for the config file
	viper.AddConfigPath("/etc/")        // config at /etc/dbtune.yaml
	viper.AddConfigPath("/etc/dbtune/") // config at /etc/dbtune/dbtune.yaml
	if pd := os.Getenv("PROGRAMDATA"); pd != "" {
		viper.AddConfigPath(filepath.Join(pd, "dbtune")) // %PROGRAMDATA%\dbtune\dbtune.yaml (Windows system-wide)
	}
	if ad := os.Getenv("APPDATA"); ad != "" {
		viper.AddConfigPath(filepath.Join(ad, "dbtune")) // %APPDATA%\dbtune\dbtune.yaml (Windows per-user)
	}
	viper.AddConfigPath(".") // optionally look for config in the working directory

	// Read the configuration file
	if err := viper.ReadInConfig(); err != nil {
		var configNotFoundErr viper.ConfigFileNotFoundError
		if errors.As(err, &configNotFoundErr) {
			// Config file not found; ignore the error
			log.Println("No config file found, proceeding with environment variables only.")
		} else {
			// Config file was found but another error occurred
			log.Fatalf("Error reading config file, %s", err)
		}
	}

	viper.AutomaticEnv()      // Read also environment variables
	viper.SetEnvPrefix("DBT") // Set a prefix for environment variables
}
