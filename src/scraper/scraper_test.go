package scraper_test

import (
	"fmt"
	"testing"

	"github.com/djfinnoy/go-scraper/src/config"
	"github.com/djfinnoy/go-scraper/src/scraper"
)

func TestScraper(t *testing.T) {
	// Create scrapers
	config := config.NewConfig("../config/test.yaml")
	scrapers := scraper.NewScrapers(config)

	if len(scrapers) != 1 {
		t.Errorf("Expected exactly 1 scraper, got %d", len(scrapers))
	}

	//	dr := daterange.NewDateRange("2024-11-10", "2024-11-17")
	//	url := scrapers["gcusd"].GetUrl(dr)
	//	data, scrapeErr := scraper.ScrapeData(url)
	//	if scrapeErr != nil {
	//		t.Errorf("Failed to scrape: %v", scrapeErr)
	//	}
	//	writeErr := scrapers["gcusd"].Write(data)
	//	if writeErr != nil {
	//		t.Errorf("Failed to write: %v", writeErr)
	//	}

	gcusd := scrapers["gcusd"].(*scraper.ScraperFMP)
	for _, dr := range gcusd.GetDatesFromBq() {
		fmt.Printf("%s", dr.GetInterval())
	}
}
