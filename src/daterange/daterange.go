package daterange

import (
	"fmt"
	"sort"
	"time"

	"cloud.google.com/go/civil"
)

type DateRange struct {
	Start civil.DateTime
	End   civil.DateTime
}

func NewDateRange(start string, end string) DateRange {
	dStart, err := time.Parse("2006-01-02", start)
	if err != nil {
		panic(fmt.Sprintf("Invalid start date: %v", err))
	}
	dEnd, err := time.Parse("2006-01-02", end)
	if err != nil {
		panic(fmt.Sprintf("Invalid end date: %v", err))
	}

	if dEnd.Before(dStart) {
		panic(fmt.Sprintf("Invalid DateRange: end cannot predate start"))
	}
	return DateRange{
		Start: civil.DateTimeOf(dStart),
		End:   civil.DateTimeOf(dEnd),
	}
}

func (dr *DateRange) Split(maxDays int) []DateRange {
	if maxDays <= 0 {
		panic("maxDays must be positive")
	}

	var result []DateRange
	current := dr.Start
	for !current.After(dr.End) {
		nextEnd := civil.DateTime{Date: current.Date.AddDays(maxDays - 1), Time: civil.Time{}}
		if nextEnd.After(dr.End) {
			nextEnd = dr.End
		}

		result = append(result, DateRange{
			Start: current,
			End:   nextEnd,
		})

		current = civil.DateTime{Date: nextEnd.Date.AddDays(1), Time: civil.Time{}}
	}

	return result
}

// Returns the subset of DateRange(s) that are not contained within an array of other DateRanges
// Used to determine what dates are not yet present in db
func (dr *DateRange) GetMissing(otherDrs []DateRange) []DateRange {
	result := []DateRange{}

	// Ensure otherDrs is sorted from earliest to latest
	sort.Slice(otherDrs, func(i, j int) bool {
		return otherDrs[i].Start.Before(otherDrs[j].Start)
	})

	var missingBeforeStart DateRange
	var missingAfterEnd DateRange

	if firstDr := otherDrs[0]; dr.Start.Before(firstDr.Start) {
		missingBeforeStart = DateRange{
			Start: dr.Start,
			End:   civil.DateTime{Date: firstDr.Start.Date.AddDays(-1), Time: civil.Time{}},
		}
		result = append(result, missingBeforeStart)
	}

	if lastDr := otherDrs[len(otherDrs)-1]; lastDr.End.Before(dr.End) {
		missingAfterEnd = DateRange{
			Start: civil.DateTime{Date: lastDr.End.Date.AddDays(1), Time: civil.Time{}},
			End:   dr.End,
		}
		result = append(result, missingAfterEnd)
	}

	return result
}

func (dr *DateRange) GetInterval() string {
	return fmt.Sprintf(
		"%s to %s\n",
		dr.Start,
		dr.End,
	)
}
