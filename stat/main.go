package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/cockroachdb/pebble"
	"github.com/joho/godotenv"
)

type Earthquake struct {
	Time      time.Time
	Magnitude float64
	Place     string
}

type WeekStats struct {
	StartDate time.Time
	EndDate   time.Time
	Year      int
	WeekNum   int
	Counts    [7]int
}

type BlueskyConfig struct {
	Identifier string
	Password   string
}

var db *pebble.DB

func main() {
	err := godotenv.Load()
	if err != nil && !os.IsNotExist(err) {
		log.Fatal("Error loading .env file")
	}

	// Initialize Pebble database
	dbPath := filepath.Join(os.TempDir(), "earthquakestats-pebble")
	db, err = pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		fmt.Printf("Error opening Pebble database: %v\n", err)
		return
	}
	defer db.Close()

	// Download CSV file
	url := "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv"
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Unexpected status code downloading file: %d\n", resp.StatusCode)
		return
	}

	// Parse CSV data
	earthquakes, err := parseCSV(resp.Body)
	if err != nil {
		fmt.Printf("Error parsing CSV: %v\n", err)
		return
	}

	// Group earthquakes by week
	weeklyStats := groupByWeek(earthquakes)

	// Get full weeks only
	fullWeeks := getFullWeeks(weeklyStats)

	// Generate reports
	if len(fullWeeks) > 0 {
		reportData := generateReports(fullWeeks)

		// Post to Bluesky if new report is available
		if reportData.ShouldPost {
			err := postToBluesky(reportData.ReportText)
			if err != nil {
				fmt.Printf("Error posting to Bluesky: %v\n", err)
			} else {
				// Mark as posted in Pebble
				markWeekAsPosted(reportData.WeekKey)
			}
		} else {
			fmt.Println("Report for this week already posted")
		}
	} else {
		fmt.Println("No complete weeks of earthquake data available")
	}
}

// ReportData contains data for the generated report
type ReportData struct {
	WeekKey    string
	ReportText string
	ShouldPost bool
}

// Check if a week has already been posted
func wasWeekPosted(weekKey string) bool {
	_, closer, err := db.Get([]byte(weekKey))
	if errors.Is(err, pebble.ErrNotFound) {
		return false
	}
	if err != nil {
		fmt.Printf("Error checking if week was posted: %v\n", err)
		return false
	}
	defer closer.Close()
	return true
}

// Mark a week as posted
func markWeekAsPosted(weekKey string) {
	err := db.Set([]byte(weekKey), []byte("posted"), pebble.Sync)
	if err != nil {
		fmt.Printf("Error marking week as posted: %v\n", err)
	}
}

// Post the earthquake report to Bluesky
func postToBluesky(reportText string) error {
	// Get Bluesky credentials from environment variables
	bskyConfig := BlueskyConfig{
		Identifier: os.Getenv("BLUESKY_IDENTIFIER"),
		Password:   os.Getenv("BLUESKY_PASSWORD"),
	}

	if bskyConfig.Identifier == "" || bskyConfig.Password == "" {
		return fmt.Errorf("missing Bluesky credentials in environment variables")
	}

	// Create a Bluesky client
	host := os.Getenv("BLUESKY_HOST")
	if host == "" {
		host = "https://me.rasc.ch"
	}

	client := &xrpc.Client{
		Host: host,
		Auth: &xrpc.AuthInfo{},
	}

	// Log in to Bluesky
	ctx := context.Background()
	auth, err := atproto.ServerCreateSession(ctx, client, &atproto.ServerCreateSession_Input{
		Identifier: bskyConfig.Identifier,
		Password:   bskyConfig.Password,
	})
	if err != nil {
		return fmt.Errorf("failed to authenticate with Bluesky: %w", err)
	}

	// Set auth info
	client.Auth.AccessJwt = auth.AccessJwt
	client.Auth.RefreshJwt = auth.RefreshJwt
	client.Auth.Handle = auth.Handle
	client.Auth.Did = auth.Did

	// Create post
	post := &bsky.FeedPost{
		Text:      reportText,
		CreatedAt: time.Now().Format(time.RFC3339),
	}

	// Submit post
	_, err = atproto.RepoCreateRecord(ctx, client, &atproto.RepoCreateRecord_Input{
		Repo:       client.Auth.Did,
		Collection: "app.bsky.feed.post",
		Record:     &util.LexiconTypeDecoder{Val: post},
	})
	if err != nil {
		return fmt.Errorf("failed to create post: %w", err)
	}

	fmt.Println("Successfully posted earthquake report to Bluesky!")
	return nil
}

func parseCSV(r io.Reader) ([]Earthquake, error) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1

	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	var earthquakes []Earthquake
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		quakeMap := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(record) {
				quakeMap[h] = record[i]
			}
		}

		t, err := time.Parse(time.RFC3339Nano, quakeMap["time"])
		if err != nil {
			continue
		}

		mag, err := strconv.ParseFloat(quakeMap["mag"], 64)
		if err != nil {
			continue
		}

		earthquakes = append(earthquakes, Earthquake{
			Time:      t.UTC(),
			Magnitude: mag,
			Place:     quakeMap["place"],
		})
	}
	return earthquakes, nil
}

func getWeekBoundaries(t time.Time) (time.Time, time.Time, int, int) {
	// Convert to UTC
	t = t.UTC()

	// Adjust to Monday-start week (1=Monday, 0=Sunday)
	weekday := int(t.Weekday())
	if weekday == 0 { // Sunday
		weekday = 7
	}

	// Calculate the start of the week (Monday 00:00:00 UTC)
	startOfDay := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	start := startOfDay.AddDate(0, 0, -(weekday - 1))

	// End of week is Sunday 23:59:59 UTC
	end := start.AddDate(0, 0, 6).Add(23*time.Hour + 59*time.Minute + 59*time.Second)

	// Get ISO year and week number based on Thursday
	year, week := start.AddDate(0, 0, 3).ISOWeek()

	return start, end, year, week
}

func categorizeMagnitude(mag float64) int {
	switch {
	case mag < 2.0:
		return 0
	case mag < 4.0:
		return 1
	case mag < 5.0:
		return 2
	case mag < 6.0:
		return 3
	case mag < 7.0:
		return 4
	case mag < 8.0:
		return 5
	default:
		return 6
	}
}

func groupByWeek(earthquakes []Earthquake) map[string]WeekStats {
	weeklyStats := make(map[string]WeekStats)

	for _, eq := range earthquakes {
		start, end, year, weekNum := getWeekBoundaries(eq.Time)
		weekKey := fmt.Sprintf("%d-W%02d", year, weekNum)

		stats, exists := weeklyStats[weekKey]
		if !exists {
			stats = WeekStats{
				StartDate: start,
				EndDate:   end,
				Year:      year,
				WeekNum:   weekNum,
			}
		}

		category := categorizeMagnitude(eq.Magnitude)
		stats.Counts[category]++

		weeklyStats[weekKey] = stats
	}

	return weeklyStats
}

func getFullWeeks(weekStats map[string]WeekStats) map[string]WeekStats {
	now := time.Now().UTC()
	fullWeeks := make(map[string]WeekStats)

	for key, stats := range weekStats {
		// Only include weeks that have already ended
		if stats.EndDate.Before(now) {
			fullWeeks[key] = stats
		}
	}

	return fullWeeks
}

func generateReports(weeklyStats map[string]WeekStats) ReportData {
	// Sort weeks chronologically
	var weeks []string
	for week := range weeklyStats {
		weeks = append(weeks, week)
	}
	sort.Strings(weeks)

	// Only report the last week
	if len(weeks) == 0 {
		return ReportData{ShouldPost: false}
	}

	lastWeek := weeks[len(weeks)-1]
	stats := weeklyStats[lastWeek]

	// Check if this week was already posted
	if wasWeekPosted(lastWeek) {
		return ReportData{
			WeekKey:    lastWeek,
			ShouldPost: false,
		}
	}

	startTimeStr := stats.StartDate.Format(time.RFC3339)[:19] + "Z"
	endTimeStr := stats.EndDate.Format(time.RFC3339)[:19] + "Z"

	categories := []string{
		"Micro < 2.0",
		"Minor 2.0 - 3.9",
		"Light 4.0 - 4.9",
		"Moderate 5.0 - 5.9",
		"Strong 6.0 - 6.9",
		"Major 7.0 - 7.9",
		"Great >= 8.0",
	}

	// Build report text
	var reportText strings.Builder
	reportText.WriteString("Weekly Earthquake Report\n")
	reportText.WriteString(fmt.Sprintf("%s (%s - %s)\n\n", lastWeek, startTimeStr, endTimeStr))

	var total int
	for i, count := range stats.Counts {
		reportText.WriteString(fmt.Sprintf("%s: %d\n", categories[i], count))
		total += count
	}
	reportText.WriteString(fmt.Sprintf("\nTotal: %d", total))

	// Print report to console as well
	fmt.Println(reportText.String())

	return ReportData{
		WeekKey:    lastWeek,
		ReportText: reportText.String(),
		ShouldPost: true,
	}
}
