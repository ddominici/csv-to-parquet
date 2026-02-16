package converter

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"csv-to-parquet/config"

	"github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type Result struct {
	InputFile  string
	OutputFile string
	InputSize  int64
	OutputSize int64
	Err        error
}

type fieldType int

const (
	typeString fieldType = iota
	typeInt64
	typeFloat64
	typeBool
	typeDate
	typeTimestamp
)

func (t fieldType) parquetTag() string {
	switch t {
	case typeInt64:
		return "type=INT64"
	case typeFloat64:
		return "type=DOUBLE"
	case typeBool:
		return "type=BOOLEAN"
	default:
		return "type=BYTE_ARRAY, convertedtype=UTF8"
	}
}

func (t fieldType) label() string {
	switch t {
	case typeInt64:
		return "INT64"
	case typeFloat64:
		return "DOUBLE"
	case typeBool:
		return "BOOLEAN"
	default:
		return "UTF8"
	}
}

// ConvertAll processes the input path (file or directory) and returns results.
func ConvertAll(cfg *config.Config, log *logrus.Logger) []Result {
	info, err := os.Stat(cfg.InputPath)
	if err != nil {
		return []Result{{InputFile: cfg.InputPath, Err: fmt.Errorf("stat input: %w", err)}}
	}

	var files []string
	if info.IsDir() {
		matches, err := filepath.Glob(filepath.Join(cfg.InputPath, "*.csv"))
		if err != nil {
			return []Result{{InputFile: cfg.InputPath, Err: fmt.Errorf("glob: %w", err)}}
		}
		files = matches
	} else {
		files = []string{cfg.InputPath}
	}

	if len(files) == 0 {
		log.Warn("No CSV files found")
		return nil
	}

	results := make([]Result, len(files))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4) // max 4 concurrent conversions

	for i, f := range files {
		wg.Add(1)
		go func(idx int, file string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[idx] = convertFile(file, cfg, log)
		}(i, f)
	}

	wg.Wait()
	return results
}

func outputPath(inputFile string, cfg *config.Config) string {
	base := strings.TrimSuffix(filepath.Base(inputFile), filepath.Ext(inputFile))
	dir := filepath.Dir(inputFile)
	if cfg.OutputDir != "" {
		dir = cfg.OutputDir
	}
	return filepath.Join(dir, base+".parquet")
}

func convertFile(inputFile string, cfg *config.Config, log *logrus.Logger) Result {
	log.Infof("Converting %s", inputFile)

	inputInfo, err := os.Stat(inputFile)
	if err != nil {
		return Result{InputFile: inputFile, Err: err}
	}

	outFile := outputPath(inputFile, cfg)
	res := Result{InputFile: inputFile, OutputFile: outFile, InputSize: inputInfo.Size()}

	delim := ','
	if len(cfg.Delimiter) > 0 {
		delim = rune(cfg.Delimiter[0])
	}

	// Read headers and sample rows for type detection
	headers, types, err := detectSchema(inputFile, delim, cfg.SampleRows)
	if err != nil {
		res.Err = fmt.Errorf("detecting schema: %w", err)
		return res
	}

	log.Debugf("Detected schema: %v", formatSchema(headers, types))

	// Build JSON schema for parquet-go
	schema := buildJSONSchema(headers, types)

	// Create output directory if needed
	if cfg.OutputDir != "" {
		if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
			res.Err = fmt.Errorf("creating output dir: %w", err)
			return res
		}
	}

	// Write parquet
	if err := writeParquet(inputFile, outFile, delim, headers, types, schema, cfg.BatchSize, log); err != nil {
		res.Err = fmt.Errorf("writing parquet: %w", err)
		// Clean up partial output
		os.Remove(outFile)
		return res
	}

	// Verify output
	outInfo, err := os.Stat(outFile)
	if err != nil || outInfo.Size() == 0 {
		res.Err = fmt.Errorf("output verification failed")
		return res
	}
	res.OutputSize = outInfo.Size()

	// Delete original
	if cfg.DeleteOriginal {
		if err := os.Remove(inputFile); err != nil {
			log.Warnf("Failed to delete original %s: %v", inputFile, err)
		} else {
			log.Infof("Deleted original %s", inputFile)
		}
	}

	log.Infof("Converted %s → %s (%.1f MB → %.1f MB)",
		inputFile, outFile,
		float64(res.InputSize)/1024/1024,
		float64(res.OutputSize)/1024/1024)

	return res
}

func detectSchema(file string, delim rune, sampleRows int) ([]string, []fieldType, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = delim
	r.LazyQuotes = true

	headers, err := r.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("reading headers: %w", err)
	}

	// Clean headers: remove BOM, trim spaces, replace problematic chars
	for i, h := range headers {
		h = strings.TrimPrefix(h, "\xef\xbb\xbf")
		h = strings.TrimSpace(h)
		h = strings.ReplaceAll(h, " ", "_")
		h = strings.ReplaceAll(h, ".", "_")
		if h == "" {
			h = fmt.Sprintf("column_%d", i)
		}
		headers[i] = h
	}

	// Sample rows
	types := make([]fieldType, len(headers))
	for i := range types {
		types[i] = typeInt64 // start optimistic
	}

	for row := 0; row < sampleRows; row++ {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		for i, val := range record {
			if i >= len(types) {
				break
			}
			if val == "" {
				continue // skip empty — don't downgrade type
			}
			types[i] = widenType(types[i], inferType(val))
		}
	}

	return headers, types, nil
}

func inferType(val string) fieldType {
	val = strings.TrimSpace(val)
	if val == "" {
		return typeString
	}

	// Bool
	lower := strings.ToLower(val)
	if lower == "true" || lower == "false" {
		return typeBool
	}

	// Int
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return typeInt64
	}

	// Float
	if _, err := strconv.ParseFloat(val, 64); err == nil {
		return typeFloat64
	}

	// Date patterns
	dateFormats := []string{
		"2006-01-02",
		"02/01/2006",
		"01/02/2006",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		time.RFC3339,
	}
	for _, fmt := range dateFormats {
		if _, err := time.Parse(fmt, val); err == nil {
			return typeString // store dates as strings for compatibility
		}
	}

	return typeString
}

// widenType returns the wider of two types (string is widest).
func widenType(current, new fieldType) fieldType {
	if current == typeString || new == typeString {
		return typeString
	}
	if current == typeFloat64 || new == typeFloat64 {
		// int + float = float
		if current == typeBool || new == typeBool {
			return typeString
		}
		return typeFloat64
	}
	if current == typeBool && new == typeBool {
		return typeBool
	}
	if (current == typeBool) != (new == typeBool) {
		return typeString // bool + int/float → string
	}
	if current == typeInt64 && new == typeInt64 {
		return typeInt64
	}
	return typeString
}

func buildJSONSchema(headers []string, types []fieldType) string {
	fields := make([]string, len(headers))
	for i, h := range headers {
		fields[i] = fmt.Sprintf(`{"Tag":"name=%s, %s, repetitiontype=OPTIONAL"}`, h, types[i].parquetTag())
	}
	return `{"Tag":"name=parquet-go-root",` +
		`"Fields":[` + strings.Join(fields, ",") + `]}`
}

func writeParquet(inputFile, outFile string, delim rune, headers []string, types []fieldType, schema string, batchSize int, log *logrus.Logger) error {
	fw, err := local.NewLocalFileWriter(outFile)
	if err != nil {
		return fmt.Errorf("creating parquet writer: %w", err)
	}

	pw, err := writer.NewJSONWriter(schema, fw, 4)
	if err != nil {
		fw.Close()
		return fmt.Errorf("creating JSON writer: %w", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128MB row groups

	// Re-open CSV
	f, err := os.Open(inputFile)
	if err != nil {
		pw.WriteStop()
		fw.Close()
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = delim
	r.LazyQuotes = true

	// Skip header
	if _, err := r.Read(); err != nil {
		pw.WriteStop()
		fw.Close()
		return err
	}

	rowCount := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Warnf("Skipping malformed row %d: %v", rowCount+1, err)
			continue
		}

		jsonRow := recordToJSON(record, headers, types)
		if err := pw.Write(jsonRow); err != nil {
			log.Warnf("Error writing row %d: %v", rowCount+1, err)
			continue
		}

		rowCount++
		if rowCount%batchSize == 0 {
			log.Debugf("Processed %d rows", rowCount)
		}
	}

	if err := pw.WriteStop(); err != nil {
		fw.Close()
		return fmt.Errorf("finalizing parquet: %w", err)
	}
	fw.Close()

	log.Infof("Wrote %d rows to %s", rowCount, outFile)
	return nil
}

func recordToJSON(record []string, headers []string, types []fieldType) string {
	parts := make([]string, 0, len(headers))
	for i, h := range headers {
		var val string
		if i < len(record) {
			val = strings.TrimSpace(record[i])
		}

		if val == "" {
			continue // omit null/empty fields (OPTIONAL)
		}

		switch types[i] {
		case typeInt64:
			if v, err := strconv.ParseInt(val, 10, 64); err == nil {
				parts = append(parts, fmt.Sprintf(`"%s":%d`, h, v))
			}
		case typeFloat64:
			if v, err := strconv.ParseFloat(val, 64); err == nil {
				parts = append(parts, fmt.Sprintf(`"%s":%g`, h, v))
			}
		case typeBool:
			lower := strings.ToLower(val)
			parts = append(parts, fmt.Sprintf(`"%s":%s`, h, lower))
		default:
			// Escape JSON string
			escaped := strings.ReplaceAll(val, `\`, `\\`)
			escaped = strings.ReplaceAll(escaped, `"`, `\"`)
			parts = append(parts, fmt.Sprintf(`"%s":"%s"`, h, escaped))
		}
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func formatSchema(headers []string, types []fieldType) string {
	parts := make([]string, len(headers))
	for i, h := range headers {
		parts[i] = fmt.Sprintf("%s:%s", h, types[i].label())
	}
	return strings.Join(parts, ", ")
}
