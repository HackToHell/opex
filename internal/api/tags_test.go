package api

import (
	"testing"
)

func TestDedup(t *testing.T) {
	tests := []struct {
		name  string
		input []string
		want  []string
	}{
		{
			name:  "nil",
			input: nil,
			want:  nil,
		},
		{
			name:  "empty",
			input: []string{},
			want:  []string{},
		},
		{
			name:  "single",
			input: []string{"a"},
			want:  []string{"a"},
		},
		{
			name:  "no duplicates",
			input: []string{"a", "b", "c"},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "with duplicates",
			input: []string{"a", "a", "b", "b", "b", "c"},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "all same",
			input: []string{"x", "x", "x"},
			want:  []string{"x"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := dedup(tc.input)
			if len(got) != len(tc.want) {
				t.Fatalf("dedup(%v) = %v (len %d), want %v (len %d)", tc.input, got, len(got), tc.want, len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("dedup()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestIntrinsicTagsAreDefined(t *testing.T) {
	// Verify intrinsicTags are a non-empty list of expected intrinsics
	if len(intrinsicTags) == 0 {
		t.Fatal("intrinsicTags should not be empty")
	}

	expected := map[string]bool{
		"duration":        true,
		"name":            true,
		"status":          true,
		"statusMessage":   true,
		"kind":            true,
		"rootServiceName": true,
		"rootName":        true,
		"traceDuration":   true,
	}

	for _, tag := range intrinsicTags {
		if !expected[tag] {
			t.Errorf("unexpected intrinsic tag: %q", tag)
		}
	}

	for tag := range expected {
		found := false
		for _, it := range intrinsicTags {
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
