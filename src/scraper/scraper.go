package scraper

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	bq "github.com/djfinnoy/go-scraper/src/bigquery"
	cfg "github.com/djfinnoy/go-scraper/src/config"
	"github.com/djfinnoy/go-scraper/src/daterange"
	dr "github.com/djfinnoy/go-scraper/src/daterange"
)

type Scraper interface {
	GetUrl(dr.DateRange) string
	Write([]map[string]interface{}) error
}

type JsonData = []map[string]interface{}

// Create an individual scraper
func newScraper(c cfg.ScraperConfig) (Scraper, error) {
	switch c["type"] {
	case "FMP":
		return newScraperFMP(c)
	default:
		panic(fmt.Sprintf("Invalid scraper type: %s", c["type"]))
	}
}

// Create all scrapers from config
func NewScrapers(c *cfg.Config) map[string]Scraper {
	scrapers := make(map[string]Scraper)
	for _, scraperConfig := range c.Scrapers {
		name, ok := scraperConfig["name"].(string)
		if !ok {
			panic(fmt.Sprintf("Scraper config must include a name key, got: %v", scraperConfig))
		}
		scraper, err := newScraper(scraperConfig)
		if err != nil {
			panic(fmt.Sprintf("Failed to create scraper: %v", err))
		}
		scrapers[name] = scraper
	}
	return scrapers
}

// Struct for scraping https://financialmodelingprep.com
type ScraperFMP struct {
	config  cfg.ScraperConfig
	bqTable *bq.BigQueryTable
}

func newScraperFMP(c cfg.ScraperConfig) (*ScraperFMP, error) {
	table, err := c.GetBigQueryTable()
	if err != nil {
		return nil, fmt.Errorf("Failed to create scraper: %v", err)
	}

	return &ScraperFMP{
		config:  c,
		bqTable: table,
	}, nil
}

func (scr *ScraperFMP) GetUrl(dr dr.DateRange) string {
	return fmt.Sprintf(

		"%s&from=%s&to=%s",
		scr.config["url"],
		dr.Start,
		dr.End,
	)
}

func (scr *ScraperFMP) Write(data []map[string]interface{}) error {
	tzString, _ := scr.config["tz"].(string)
	tz, err := time.LoadLocation(tzString)
	if err != nil {
		return fmt.Errorf("Timezone error: string `%s` caused error: %v", tzString, err)
	}
	return scr.bqTable.Write(data, tz)
}

func (scr *ScraperFMP) getCurrentDateRanges() []daterange.DateRange {
	dates, err := scr.bqTable.GetTableDates("date")
	if err != nil {
		panic(err)
	}
	return dates
}

// Returns DateRange(s) that are not present in BigQuery table
//func (scr *ScraperFMP) GetScrapableDateRanges() []daterange.DateRange {
//	currentDateRanges := scr.getCurrentDateRanges()
//	configDateRange := scr.config.GetDateRange()
//
//
//}

// Helper functions

func ScrapeData(url string) (JsonData, error) {
	client := &http.Client{
		Timeout: time.Second * 30,
	}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error making GET request: %v", err)
	}
	defer resp.Body.Close()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}

	// Parse the JSON data
	var data JsonData
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, fmt.Errorf("error parsing JSON: %v", err)
	}

	return data, nil
}
