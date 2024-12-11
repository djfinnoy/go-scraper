package config_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/djfinnoy/go-scraper/src/config"
)

func TestConfig(t *testing.T) {

	os.Setenv("FMP_APIKEY", "kilkelly")

	c := config.NewConfig("./test.yaml")
	if len(c.Scrapers) != 1 {
		t.Error(fmt.Sprintf("Expected an array of length 1, got %d", len(c.Scrapers)))
	}

	endDate, ok := c.Scrapers[0]["endDate"].(string)
	if !ok {
		t.Error("`endDate` not found in scraper config")
		return
	} else if endDate != "yesterday" {
		t.Errorf("Expected `endDate` to equal `yesterday`, got %s", endDate)
	}

	apikey, ok := c.Scrapers[0]["apiKey"].(string)
	if !ok {
		t.Error("`apiKey` not found in scraper config")
	} else if apikey != "kilkelly" {
		t.Errorf("Expected `apiKey` to equal `kilkelly`, got %s", apikey)
	}

	url, ok := c.Scrapers[0]["url"].(string)
	if !ok {
		t.Error("`url` not found in scraper config")
	} else if url != "https://financialmodelingprep.com/api/v3/historical-chart/5min/gcusd?apikey=kilkelly" {
		t.Errorf("Expected `url` to equal `https://financialmodelingprep.com/api/v3/historical-chart/5min/gcusd?apikey=kilkelly`, got %s", url)
	}
}
