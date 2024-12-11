package daterange_test

import (
	"fmt"
	"testing"

	"github.com/djfinnoy/go-scraper/src/daterange"
)

func TestDateRange(t *testing.T) {

	// Split a daterange into multiple dateranges
	dr := daterange.NewDateRange("2024-01-01", "2024-01-06")
	drs1 := dr.Split(1)

	if len(drs1) != 6 {
		t.Error(fmt.Sprintf("Expected an array of length 6, got %d", len(drs1)))
		for _, d := range drs1 {
			fmt.Printf("%s\n", d.GetInterval())
		}
	}

	drs2 := dr.Split(2)
	if len(drs2) != 3 {
		t.Error(fmt.Sprintf("Expected an array of length 3, got %d", len(drs2)))
		for _, d := range drs2 {
			fmt.Printf("%s\n", d.GetInterval())
		}
	}

	// Get a list of dateranges defined in config but missing from database
	configDr := daterange.NewDateRange("2024-01-01", "2024-03-31")
	dbDrs := []daterange.DateRange{
		daterange.NewDateRange("2024-01-03", "2024-01-07"),
		daterange.NewDateRange("2024-01-09", "2024-03-30"),
	}

	// TODO: actually test this
	missing := configDr.GetMissing(dbDrs)
	if dr := missing[0].GetInterval(); dr != "2024-01-01T00:00:00 to 2024-01-02T00:00:00\n" {
		t.Errorf("unexpected missing daterange, got: %v", dr)
	}
	if dr := missing[1].GetInterval(); dr != "2024-03-31T00:00:00 to 2024-03-31T00:00:00\n" {
		t.Errorf("unexpected missing daterange, got: %v", dr)
	}
}
