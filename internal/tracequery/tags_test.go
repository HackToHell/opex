package tracequery

import (
	"testing"
	"time"
)

func TestValidAttributeName(t *testing.T) {
	valid := []string{
		"http.method",
		"service.name",
		"resource.service.name",
		"http_status_code",
		"_private",
		"a",
	}
	for _, name := range valid {
		if !validAttributeName.MatchString(name) {
			t.Errorf("expected %q to be valid", name)
		}
	}

	invalid := []string{
		"",
		"123abc",            // starts with digit
		"'; DROP TABLE--",   // SQL injection
		"tag name",          // space
		"tag-name",          // hyphen
		"tag$name",          // dollar
		"a' OR '1'='1",     // injection
	}
	for _, name := range invalid {
		if validAttributeName.MatchString(name) {
			t.Errorf("expected %q to be invalid", name)
		}
	}
}

func TestIntrinsicTagsAreDefined(t *testing.T) {
	if len(IntrinsicTags) == 0 {
		t.Fatal("IntrinsicTags should not be empty")
	}

	expected := map[string]bool{
		"duration": true, "name": true, "status": true,
		"statusMessage": true, "kind": true, "rootServiceName": true,
		"rootName": true, "traceDuration": true,
	}

	for _, tag := range IntrinsicTags {
		if !expected[tag] {
			t.Errorf("unexpected intrinsic tag: %q", tag)
		}
	}
	for tag := range expected {
		found := false
		for _, it := range IntrinsicTags {
			if it == tag {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing expected intrinsic tag: %q", tag)
		}
	}
}

func TestEscapeSQLInTagValues(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"normal.tag", "normal.tag"},
		{"tag'inject", "tag\\'inject"},
		{`tag\escape`, `tag\\escape`},
	}
	for _, tc := range tests {
		got := escapeSQL(tc.input)
		if got != tc.want {
			t.Errorf("escapeSQL(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestSnapTo5m(t *testing.T) {
	utc := time.UTC
	tests := []struct {
		name      string
		start     time.Time
		end       time.Time
		wantStart time.Time
		wantEnd   time.Time
	}{
		{
			name:      "already aligned",
			start:     time.Date(2025, 1, 15, 10, 0, 0, 0, utc),
			end:       time.Date(2025, 1, 15, 10, 15, 0, 0, utc),
			wantStart: time.Date(2025, 1, 15, 10, 0, 0, 0, utc),
			wantEnd:   time.Date(2025, 1, 15, 10, 15, 0, 0, utc),
		},
		{
			name:      "start unaligned",
			start:     time.Date(2025, 1, 15, 10, 2, 0, 0, utc),
			end:       time.Date(2025, 1, 15, 10, 15, 0, 0, utc),
			wantStart: time.Date(2025, 1, 15, 10, 0, 0, 0, utc),
			wantEnd:   time.Date(2025, 1, 15, 10, 15, 0, 0, utc),
		},
		{
			name:      "end unaligned",
			start:     time.Date(2025, 1, 15, 10, 0, 0, 0, utc),
			end:       time.Date(2025, 1, 15, 10, 13, 0, 0, utc),
			wantStart: time.Date(2025, 1, 15, 10, 0, 0, 0, utc),
			wantEnd:   time.Date(2025, 1, 15, 10, 15, 0, 0, utc),
		},
		{
			name:      "both unaligned",
			start:     time.Date(2025, 1, 15, 10, 2, 0, 0, utc),
			end:       time.Date(2025, 1, 15, 10, 13, 0, 0, utc),
			wantStart: time.Date(2025, 1, 15, 10, 0, 0, 0, utc),
			wantEnd:   time.Date(2025, 1, 15, 10, 15, 0, 0, utc),
		},
		{
			name:      "end on boundary stays exclusive",
			start:     time.Date(2025, 1, 15, 10, 0, 0, 0, utc),
			end:       time.Date(2025, 1, 15, 10, 10, 0, 0, utc),
			wantStart: time.Date(2025, 1, 15, 10, 0, 0, 0, utc),
			wantEnd:   time.Date(2025, 1, 15, 10, 10, 0, 0, utc),
		},
		{
			name:      "cross midnight",
			start:     time.Date(2025, 1, 15, 23, 58, 0, 0, utc),
			end:       time.Date(2025, 1, 16, 0, 3, 0, 0, utc),
			wantStart: time.Date(2025, 1, 15, 23, 55, 0, 0, utc),
			wantEnd:   time.Date(2025, 1, 16, 0, 5, 0, 0, utc),
		},
		{
			name:      "14 day window",
			start:     time.Date(2025, 1, 1, 0, 1, 0, 0, utc),
			end:       time.Date(2025, 1, 15, 0, 1, 0, 0, utc),
			wantStart: time.Date(2025, 1, 1, 0, 0, 0, 0, utc),
			wantEnd:   time.Date(2025, 1, 15, 0, 5, 0, 0, utc),
		},
		{
			name:      "sub-second precision snaps outward",
			start:     time.Date(2025, 1, 15, 10, 0, 0, 1, utc),
			end:       time.Date(2025, 1, 15, 10, 4, 59, 999999999, utc),
			wantStart: time.Date(2025, 1, 15, 10, 0, 0, 0, utc),
			wantEnd:   time.Date(2025, 1, 15, 10, 5, 0, 0, utc),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotStart, gotEnd := snapTo5m(tc.start, tc.end)
			if !gotStart.Equal(tc.wantStart) {
				t.Errorf("snappedStart = %v, want %v", gotStart, tc.wantStart)
			}
			if !gotEnd.Equal(tc.wantEnd) {
				t.Errorf("snappedEnd = %v, want %v", gotEnd, tc.wantEnd)
			}
		})
	}
}
