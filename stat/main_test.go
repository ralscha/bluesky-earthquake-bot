package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseCSVUsesHeadersAndSkipsShortRows(t *testing.T) {
	csv := `time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type,status
2026-06-08T10:11:12.345Z,1,2,3,4.4,ml,,,,,us,us123,,10 km S of Test,earthquake,reviewed
2026-06-08T10:11:12Z,1,2,3
`

	quakes, err := parseCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("parseCSV returned error: %v", err)
	}

	if len(quakes) != 1 {
		t.Fatalf("expected 1 parsed earthquake, got %d", len(quakes))
	}
	if quakes[0].Magnitude != 4.4 {
		t.Fatalf("expected magnitude 4.4, got %f", quakes[0].Magnitude)
	}
	if quakes[0].Place != "10 km S of Test" {
		t.Fatalf("expected place from header mapping, got %q", quakes[0].Place)
	}
	if quakes[0].Time.Location() != time.UTC {
		t.Fatalf("expected UTC time, got %v", quakes[0].Time.Location())
	}
}
