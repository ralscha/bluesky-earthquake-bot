package main

import (
	"strings"
	"testing"
)

func TestParseEarthquakesCSVHandlesFractionalSecondsAndShortRows(t *testing.T) {
	csv := `time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type,status
2026-06-08T10:11:12.345Z,1,2,3,5.6,mww,,,,,us,us123,,10 km S of Test,earthquake,reviewed
2026-06-08T10:11:12Z,1,2,3
`

	quakes, err := parseEarthquakesCSV(strings.NewReader(csv), true)
	if err != nil {
		t.Fatalf("parseEarthquakesCSV returned error: %v", err)
	}

	if len(quakes) != 1 {
		t.Fatalf("expected 1 parsed earthquake, got %d", len(quakes))
	}
	if quakes[0].ID != "us123" {
		t.Fatalf("expected ID us123, got %q", quakes[0].ID)
	}
	if !quakes[0].IsSignificant {
		t.Fatal("expected significant flag to be preserved")
	}
}
