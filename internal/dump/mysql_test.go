package dump

import (
	"testing"
)

func TestParseString(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{
			name:  "plain string unchanged",
			input: []byte("hello world"),
			want:  "hello world",
		},
		{
			name:  "single quote becomes double single quote",
			input: []byte("it's"),
			want:  "it''s",
		},
		{
			name:  "backslash doubled",
			input: []byte(`C:\path`),
			want:  `C:\\path`,
		},
		{
			name:  "newline escaped",
			input: []byte("line1\nline2"),
			want:  `line1\nline2`,
		},
		{
			name:  "carriage return escaped",
			input: []byte("line1\rline2"),
			want:  `line1\rline2`,
		},
		{
			name:  "null byte escaped",
			input: []byte{104, 0, 105}, // "h\0i"
			want:  `h\0i`,
		},
		{
			name:  "ctrl-z escaped",
			input: []byte{104, 26, 105}, // "h\Zi"
			want:  `h\Zi`,
		},
		{
			name:  "multiple special chars",
			input: []byte("it's a \"test\"\n"),
			want:  "it''s a \"test\"\\n", // double-quote not escaped; newline → \n (two chars)
		},
		{
			name:  "empty input",
			input: []byte{},
			want:  "",
		},
		{
			name:  "only special chars",
			input: []byte("'\\\n\r"),
			want:  `''\\\n\r`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(ParseString(tt.input))
			if got != tt.want {
				t.Errorf("ParseString(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

