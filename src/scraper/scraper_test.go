package scraper_test

import (
	"testing"

	"github.com/djfinnoy/go-scraper/src/config"
	"github.com/djfinnoy/go-scraper/src/daterange"
	"github.com/djfinnoy/go-scraper/src/scraper"
)

func TestScraper(t *testing.T) {
	// Create scrapers
	config := config.NewConfig("../config/test.yaml")
	scrapers := scraper.NewScrapers(config)

	if len(scrapers) != 1 {
		t.Errorf("expected exactly 1 scraper, got %d", len(scrapers))
	}

	scraper := scrapers["gcusd"]
	dr := daterange.NewDateRange("2024-11-10", "2024-11-17")
	data, err := scraper.Read(dr)
	if err != nil {
		t.Errorf("read failed: %v", err)
	}

	// Mock a bigquery table with multiple scenarios with respect to existing data
	// - test that we don't write data that is already present
	// - test that we do write data that is missing

	if err := scraper.Write(data); err != nil {
		t.Errorf("write failed: %v", err)
	}

	// data, scrapeErr := scraper.ScrapeData(url)
	//
	//	if scrapeErr != nil {
	//		t.Errorf("Failed to scrape: %v", scrapeErr)
	//	}
	//
	// writeErr := scrapers["gcusd"].Write(data)
	//
	//	if writeErr != nil {
	//		t.Errorf("Failed to write: %v", writeErr)
	//	}
	//
	// gcusd := scrapers["gcusd"].(*scraper.ScraperFMP)
	// derp := gcusd.GetDateRangesNotPresentInBigQuery()
	//
	//	for _, dr := range derp {
	//		fmt.Printf("%s", dr.GetInterval())
	//	}
}
