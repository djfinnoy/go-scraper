package config

import (
	"bytes"
	"fmt"
	"html/template"
	"os"
	"regexp"
	"strings"
	"time"

	bq "github.com/djfinnoy/go-scraper/src/bigquery"
	dr "github.com/djfinnoy/go-scraper/src/daterange"
	"gopkg.in/yaml.v3"
)

type ScraperConfig map[string]interface{}

type Config struct {
	Scrapers []ScraperConfig
}

type DefaultsYaml struct {
	Data map[string]interface{} `yaml:"defaults"`
}

type ScrapersYaml struct {
	Data []map[string]interface{} `yaml:"scrapers"`
}

func NewConfig(path string) *Config {
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("Can't read yaml file %s", path))
	}

	var defaults DefaultsYaml
	err = yaml.Unmarshal(yamlFile, &defaults)
	if err != nil {
		panic(fmt.Errorf("Failed to parse defaults from YAML: %v", err))
	}

	var scrapers ScrapersYaml
	err = yaml.Unmarshal(yamlFile, &scrapers)
	if err != nil {
		panic(fmt.Errorf("Failed to parse defaults from YAML: %v", err))
	}

	// Merge `defaults.scrapers` with each scraper in `scrapers`
	for i, scraper := range scrapers.Data {
		// Check if 'scrapers' key exists in defaults
		if defaultScrapers, ok := defaults.Data["scrapers"].(map[string]interface{}); ok {
			for key, defaultValue := range defaultScrapers {
				// If a key is not defined, inject the default value
				if _, exists := scraper[key]; !exists {
					scrapers.Data[i][key] = defaultValue
				}
			}
		}

		// Parse environment variables
		for key, value := range scraper {
			if str, ok := value.(string); ok {
				// Parse environment variables
				if strings.Contains(str, "$") {
					scraper[key] = parseEnvironmentVar(str)
				}
			}
		}

		// Parse template strings
		for key, value := range scraper {
			if str, ok := value.(string); ok {
				// Parse template strings
				if strings.Contains(str, "{{") {
					scraper[key] = parseTemplate(str, scraper)
				}
			}
		}

	}

	var scraperConfigs []ScraperConfig
	for _, data := range scrapers.Data {
		scraperConfigs = append(scraperConfigs, ScraperConfig(data))
	}

	return &Config{
		Scrapers: scraperConfigs,
	}
}

// Methods

func (c ScraperConfig) GetDateRange() dr.DateRange {
	startDate, ok := c["startDate"].(string)
	if !ok {
		panic(fmt.Sprintf("`startDate` not defined in scraper config: %v", c))
	}

	var endDate string
	if end, ok := c["endDate"].(string); ok {
		if end == "yesterday" {
			endDate = time.Now().AddDate(0, 0, -1).Format("2006-01-02")
		} else {
			endDate = end
		}
	} else {
		panic(fmt.Sprintf("`endDate` not defined in scraper config: %v", c))
	}

	return dr.NewDateRange(startDate, endDate)
}

func (c ScraperConfig) GetBigQueryTable() (*bq.BigQueryTable, error) {
	cfg, ok := c["destination"].(map[string]interface{})["bigQuery"].(map[string]interface{})
	if !ok {
		panic(fmt.Sprintf("`bigQuery` key is missing, or contains invalid values: %v", cfg))
	}

	project, ok := cfg["project"].(string)
	if !ok {
		panic(fmt.Sprintf("`bigQuery.project` key is missing: %v", cfg))
	}

	dataset, ok := cfg["dataset"].(string)
	if !ok {
		panic(fmt.Sprintf("`bigQuery.dataset` key is missing: %v", cfg))
	}

	table, ok := cfg["table"].(string)
	if !ok {
		panic(fmt.Sprintf("`bigQuery.table` key is missing: %v", cfg))
	}

	return bq.NewBigQueryTable(project, dataset, table)
}

// Helper functions

func parseTemplate(tmpl string, data map[string]interface{}) string {
	t, err := template.New("").Parse(tmpl)
	if err != nil {
		panic(err)
	}

	var buf bytes.Buffer
	err = t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}

	return buf.String()
}

func parseEnvironmentVar(str string) string {
	re := regexp.MustCompile(`\$([A-Za-z0-9_]+)`)
	return re.ReplaceAllStringFunc(str, func(match string) string {
		envVar := strings.TrimPrefix(match, "$")
		return os.Getenv(envVar)
	})
}
