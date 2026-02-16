package main

import (
	"fmt"
	"os"

	"csv-to-parquet/config"
	"csv-to-parquet/converter"

	"github.com/sirupsen/logrus"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	log := logrus.New()
	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = logrus.InfoLevel
	}
	log.SetLevel(level)
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	log.Infof("csv-to-parquet starting — input: %s", cfg.InputPath)

	results := converter.ConvertAll(cfg, log)

	// Summary
	var converted, failed int
	var totalInputSize, totalOutputSize int64
	for _, r := range results {
		if r.Err != nil {
			failed++
			log.Errorf("FAILED %s: %v", r.InputFile, r.Err)
		} else {
			converted++
			totalInputSize += r.InputSize
			totalOutputSize += r.OutputSize
		}
	}

	log.Infof("Done: %d converted, %d failed", converted, failed)
	if converted > 0 {
		saved := totalInputSize - totalOutputSize
		log.Infof("Space: %.1f MB input → %.1f MB parquet (%.1f MB saved)",
			float64(totalInputSize)/1024/1024,
			float64(totalOutputSize)/1024/1024,
			float64(saved)/1024/1024)
	}

	if failed > 0 {
		os.Exit(1)
	}
}
