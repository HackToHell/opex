package tracequery

import (
	"testing"
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
	// Verify escapeSQL is applied in queryTagValues SQL generation path
	// by testing the escape function directly with injection payloads
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
