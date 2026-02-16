package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	flag "github.com/spf13/pflag"
)

type Config struct {
	InputPath      string `yaml:"input"`
	OutputDir      string `yaml:"output"`
	DeleteOriginal bool   `yaml:"delete_original"`
	LogLevel       string `yaml:"log_level"`
	BatchSize      int    `yaml:"batch_size"`
	Delimiter      string `yaml:"delimiter"`
	SampleRows     int    `yaml:"sample_rows"`
}

func Load() (*Config, error) {
	// Defaults
	cfg := &Config{
		DeleteOriginal: true,
		LogLevel:       "info",
		BatchSize:      10000,
		Delimiter:      ",",
		SampleRows:     100,
	}

	// CLI flags
	configPath := flag.String("config", "config.yaml", "Path to config file")
	input := flag.String("input", "", "Input CSV file or directory")
	output := flag.String("output", "", "Output directory (default: same as input)")
	keep := flag.Bool("keep", false, "Keep original CSV files after conversion")
	logLevel := flag.String("log-level", "", "Log level (debug, info, warn, error)")
	batchSize := flag.Int("batch-size", 0, "Rows per row group")
	delimiter := flag.String("delimiter", "", "CSV delimiter character")
	sampleRows := flag.Int("sample-rows", 0, "Number of rows to sample for type detection")

	flag.Parse()

	// Load config file
	data, err := os.ReadFile(*configPath)
	if err != nil {
		if *configPath != "config.yaml" {
			return nil, fmt.Errorf("reading config file %s: %w", *configPath, err)
		}
		// Default config file missing is fine
	} else {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parsing config file: %w", err)
		}
	}

	// CLI flags override config file
	if *input != "" {
		cfg.InputPath = *input
	}
	if *output != "" {
		cfg.OutputDir = *output
	}
	if *keep {
		cfg.DeleteOriginal = false
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}
	if *batchSize > 0 {
		cfg.BatchSize = *batchSize
	}
	if *delimiter != "" {
		cfg.Delimiter = *delimiter
	}
	if *sampleRows > 0 {
		cfg.SampleRows = *sampleRows
	}

	if cfg.InputPath == "" {
		return nil, fmt.Errorf("input path is required (use --input flag or set in config)")
	}

	return cfg, nil
}
