package parser

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// MetricSample represents a single parsed Prometheus metric sample
type MetricSample struct {
	Name      string
	Labels    map[string]string
	Value     float64
	Timestamp time.Time
}

// ParsePrometheusText parses Prometheus text exposition format lines
func ParsePrometheusText(r io.Reader) ([]MetricSample, error) {
	scanner := bufio.NewScanner(r)
	var samples []MetricSample
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			// Skip empty lines and comments (HELP/TYPE)
			continue
		}

		sample, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum, err)
		}
		samples = append(samples, sample)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return samples, nil
}

func parseLine(line string) (MetricSample, error) {
	var sample MetricSample
	sample.Labels = make(map[string]string)

	openBrace := strings.IndexByte(line, '{')
	closeBrace := strings.LastIndexByte(line, '}')

	var metricName string
	var rest string

	if openBrace != -1 && closeBrace != -1 && closeBrace > openBrace {
		metricName = strings.TrimSpace(line[:openBrace])
		labelPart := line[openBrace+1 : closeBrace]
		rest = strings.TrimSpace(line[closeBrace+1:])

		// parse labels
		pairs := splitLabels(labelPart)
		for _, pair := range pairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				k := strings.TrimSpace(kv[0])
				v := strings.Trim(strings.TrimSpace(kv[1]), "\"")
				sample.Labels[k] = v
			}
		}
	} else {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return sample, fmt.Errorf("invalid format: %s", line)
		}
		metricName = fields[0]
		rest = strings.Join(fields[1:], " ")
	}

	sample.Name = metricName
	sample.Labels["__name__"] = metricName

	restFields := strings.Fields(rest)
	if len(restFields) == 0 {
		return sample, fmt.Errorf("missing value in line: %s", line)
	}

	val, err := strconv.ParseFloat(restFields[0], 64)
	if err != nil {
		return sample, fmt.Errorf("invalid metric value %q: %w", restFields[0], err)
	}
	sample.Value = val

	if len(restFields) >= 2 {
		tsMillis, err := strconv.ParseInt(restFields[1], 10, 64)
		if err == nil {
			sample.Timestamp = time.UnixMilli(tsMillis)
		} else {
			sample.Timestamp = time.Now()
		}
	} else {
		sample.Timestamp = time.Now()
	}

	return sample, nil
}

func splitLabels(labelStr string) []string {
	var pairs []string
	var current strings.Builder
	inQuote := false

	for i := 0; i < len(labelStr); i++ {
		c := labelStr[i]
		if c == '"' {
			inQuote = !inQuote
		}
		if c == ',' && !inQuote {
			if current.Len() > 0 {
				pairs = append(pairs, current.String())
				current.Reset()
			}
			continue
		}
		current.WriteByte(c)
	}
	if current.Len() > 0 {
		pairs = append(pairs, current.String())
	}
	return pairs
}
