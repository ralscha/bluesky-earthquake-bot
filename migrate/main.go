package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/cockroachdb/pebble"
)

type Earthquake struct {
	ID  string
	Mag float64
}

func main() {
	earthquakeData, err := downloadAndParseCSV()
	if err != nil {
		log.Fatal("Failed to download and parse CSV:", err)
	}

	log.Printf("Downloaded %d earthquakes from CSV", len(earthquakeData))

	oldDB, err := pebble.Open("quake-db", &pebble.Options{
		ReadOnly: true,
	})
	if err != nil {
		log.Fatal("Failed to open old database:", err)
	}
	defer oldDB.Close()

	if err := os.RemoveAll("quake-db-new"); err != nil {
		log.Printf("Warning: Failed to remove existing new database: %v", err)
	}

	newDB, err := pebble.Open("quake-db-new", &pebble.Options{})
	if err != nil {
		log.Fatal("Failed to create new database:", err)
	}
	defer newDB.Close()

	// Only copy entries that exist in the CSV file
	iter, err := oldDB.NewIter(nil)
	if err != nil {
		log.Fatal("Failed to create iterator:", err)
	}
	defer iter.Close()

	migratedCount := 0
	skippedCount := 0

	for iter.First(); iter.Valid(); iter.Next() {
		earthquakeID := string(iter.Key())

		earthquake, exists := earthquakeData[earthquakeID]
		if !exists {
			log.Printf("Skipping earthquake ID %s (not found in CSV)", earthquakeID)
			skippedCount++
			continue
		}

		magnitudeBytes := fmt.Appendf(nil, "%.1f", earthquake.Mag)

		if err := newDB.Set(iter.Key(), magnitudeBytes, &pebble.WriteOptions{}); err != nil {
			log.Printf("Failed to store earthquake ID %s: %v", earthquakeID, err)
			continue
		}

		log.Printf("Migrated earthquake ID %s with magnitude %.1f", earthquakeID, earthquake.Mag)
		migratedCount++
	}

	if err := iter.Error(); err != nil {
		log.Fatal("Iterator error:", err)
	}

	if err := newDB.Flush(); err != nil {
		log.Fatal("Failed to flush new database:", err)
	}

	log.Printf("Migration completed successfully!")
	log.Printf("Migrated: %d entries", migratedCount)
	log.Printf("Skipped: %d entries (not in CSV)", skippedCount)
	log.Printf("New database created at: quake-db-new")
}

func downloadAndParseCSV() (map[string]Earthquake, error) {
	resp, err := http.Get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.csv")
	if err != nil {
		return nil, fmt.Errorf("failed to download CSV: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	reader := csv.NewReader(resp.Body)
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	earthquakeData := make(map[string]Earthquake)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV record: %w", err)
		}

		quakeMap := make(map[string]string)
		for i, h := range headers {
			if i < len(record) {
				quakeMap[h] = record[i]
			}
		}

		mag, err := strconv.ParseFloat(quakeMap["mag"], 64)
		if err != nil {
			log.Printf("Skipping invalid magnitude '%s' for ID %s", quakeMap["mag"], quakeMap["id"])
			continue
		}

		earthquakeData[quakeMap["id"]] = Earthquake{
			ID:  quakeMap["id"],
			Mag: mag,
		}
	}

	return earthquakeData, nil
}
