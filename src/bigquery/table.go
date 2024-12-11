package bigquery

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/djfinnoy/go-scraper/src/daterange"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type BigQueryTable struct {
	client *bigquery.Client
	table  *bigquery.Table
}

type BigQueryRow map[string]bigquery.Value

func NewBigQueryTable(project string, dataset string, table string) (*BigQueryTable, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
	}

	ds := client.Dataset(dataset)
	tbl := ds.Table(table)

	// Check if the table exists
	_, err = tbl.Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
			// Create the table if it doesn't exist
			tableMetadata := &bigquery.TableMetadata{
				Schema: bigquery.Schema{}, // Empty schema
			}
			if err := tbl.Create(ctx, tableMetadata); err != nil {
				client.Close()
				return nil, fmt.Errorf("failed to create table: %v", err)
			}
		} else {
			// If it's a different error, return it
			client.Close()
			return nil, fmt.Errorf("failed to get table metadata: %v", err)
		}
	}

	return &BigQueryTable{
		client: client,
		table:  tbl,
	}, nil
}

// Methods

func (bq *BigQueryTable) Write(jsonData []map[string]interface{}, tz *time.Location) error {
	data := jsonToBigQueryRows(jsonData, tz)

	if len(data) == 0 {
		return fmt.Errorf("`data` is empty")
	}

	inserter := bq.table.Inserter()
	var err error
	maxRetries := 10
	handledMissingSchema := false

	for attempt := 0; attempt < maxRetries; attempt++ {
		err = inserter.Put(context.Background(), data)
		if err == nil {
			fmt.Println("Data successfully inserted into BigQuery")
			return nil
		}

		if apiErr, ok := err.(*googleapi.Error); ok && apiErr.Code == 400 &&
			strings.Contains(apiErr.Message, "The destination table has no schema") && !handledMissingSchema {
			// Handle the "no schema" error by setting the schema, but only if we haven't set it yet
			err = bq.setTableSchema(data[0])
			if err != nil {
				return fmt.Errorf("failed to set table schema: %v", err)
			}
			fmt.Println("Table schema successfully set")
			handledMissingSchema = true
		} else {
			fmt.Printf("Failed to insert data. Retrying in 1 minute (attempt %d of %d): %v\n", attempt+1, maxRetries, err)
		}

		time.Sleep(time.Minute)
	}

	return fmt.Errorf("failed to insert data after %d attempts: %v", maxRetries, err)
}

func (bq *BigQueryTable) GetTableDates(colname string) ([]daterange.DateRange, error) {
	ctx := context.Background()
	tbl := fmt.Sprintf("%s.%s.%s", bq.table.ProjectID, bq.table.DatasetID, bq.table.TableID)

	queryTemplate, err := template.New("").Parse(`
		WITH base AS (
		  SELECT
		    {{.colname}},
		    TIMESTAMP_DIFF({{.colname}}, LAG({{.colname}}) OVER (ORDER BY {{.colname}}), MINUTE) AS tdiff,
		  FROM {{.tbl}}
		),

		define_groups AS (
		  SELECT
		    *,
		    SUM(CASE WHEN tdiff >= 1440 THEN 1 ELSE 0 END) OVER (ORDER BY {{.colname}}) AS grp
		  FROM base
		),

		summarize_groups AS (
		  SELECT
		    MIN({{.colname}}) AS start_date,
		    MAX({{.colname}}) AS end_date
		  FROM define_groups
		  GROUP BY grp
		)

		SELECT * FROM summarize_groups
		ORDER BY start_date
	`)

	if err != nil {
		return nil, fmt.Errorf("Error creating query template: %v", err)
	}

	params := map[string]string{
		"colname": colname,
		"tbl":     tbl,
	}

	var buf bytes.Buffer
	err = queryTemplate.Execute(&buf, params)
	if err != nil {
		return nil, fmt.Errorf("Error parsing query template: %v", err)
	}

	query := bq.client.Query(buf.String())

	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to run query: %v", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to wait for job completion: %v", err)
	}

	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("Job failed: %v", err)
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to read job results: %v", err)
	}

	var dateRanges []daterange.DateRange
	rowNum := 0
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Error reading row: %v", err)
		}

		if len(values) != 2 {
			return nil, fmt.Errorf("Expected 2 columns, got %d", len(values))
		}

		startDate, ok := values[0].(civil.DateTime)
		if !ok {
			return nil, fmt.Errorf("Start date is not a civil.DateTime: %v", values[0])
		}

		endDate, ok := values[1].(civil.DateTime)
		if !ok {
			return nil, fmt.Errorf("End date is not a civil.DateTime: %v", values[1])
		}

		dateRange := daterange.DateRange{
			Start: startDate,
			End:   endDate,
		}
		dateRanges = append(dateRanges, dateRange)

		rowNum++
	}

	return dateRanges, nil
}

// Needed to interface with bigquery package
func (r BigQueryRow) Save() (map[string]bigquery.Value, string, error) {
	return r, "", nil
}

// Convert json data to a writable format for BigQuery
func jsonToBigQueryRows(jsonData []map[string]interface{}, tz *time.Location) []BigQueryRow {
	var data []BigQueryRow

	for _, item := range jsonData {
		bqItem := make(BigQueryRow)
		for k, v := range item {
			if str, ok := v.(string); ok {
				// Capture datetime columns that appear as strings in the json data
				if t, err := time.ParseInLocation("2006-01-02 15:04:05", str, tz); err == nil {
					// Convert to civil.DateTime for BigQuery, keeping the original timezone
					year, month, day := t.Date()
					hour, min, sec := t.Clock()
					civilDateTime := civil.DateTime{
						Date: civil.Date{Year: year, Month: month, Day: day},
						Time: civil.Time{Hour: hour, Minute: min, Second: sec},
					}
					bqItem[k] = civilDateTime
				} else {
					bqItem[k] = str
				}
			} else {
				bqItem[k] = v
			}
		}
		data = append(data, bqItem)
	}
	return data
}

func (bq *BigQueryTable) setTableSchema(sampleRow BigQueryRow) error {
	schema := make(bigquery.Schema, 0, len(sampleRow))
	for fieldName, value := range sampleRow {
		fieldType := bigquery.StringFieldType // default type
		switch value.(type) {
		case int64:
			fieldType = bigquery.IntegerFieldType
		case float64:
			fieldType = bigquery.FloatFieldType
		case bool:
			fieldType = bigquery.BooleanFieldType
		case time.Time:
			fieldType = bigquery.TimestampFieldType
		case []byte:
			fieldType = bigquery.BytesFieldType
		case civil.Date:
			fieldType = bigquery.DateFieldType
		case civil.Time:
			fieldType = bigquery.TimeFieldType
		case civil.DateTime:
			fieldType = bigquery.DateTimeFieldType
		}
		schema = append(schema, &bigquery.FieldSchema{Name: fieldName, Type: fieldType})
	}

	_, err := bq.table.Update(context.Background(), bigquery.TableMetadataToUpdate{Schema: schema}, "")
	if err != nil {
		return fmt.Errorf("failed to update table schema: %v", err)
	}

	fmt.Println("Table schema successfully set")
	return nil
}
