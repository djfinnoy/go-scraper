package main

import (
	"fmt"

	"github.com/djfinnoy/go-scraper/src/config"
	"github.com/djfinnoy/go-scraper/src/scraper"
)

func main() {

	config := config.NewConfig("../config/test.yaml")
	scrapers := scraper.NewScrapers(config)

	fmt.Println(len(scrapers)) // TODO: remove
}
