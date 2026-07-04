package dump

import (
	"strings"
	"testing"
)

func TestStripDefiner(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{
			name: "backtick quoted",
			in:   "CREATE DEFINER=`app`@`10.0.%` TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET @a=1",
			want: "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET @a=1",
		},
		{
			name: "unquoted user and host",
			in:   "CREATE DEFINER=root@localhost PROCEDURE p() BEGIN END",
			want: "CREATE PROCEDURE p() BEGIN END",
		},
		{
			name: "single quoted",
			in:   "CREATE DEFINER='app user'@'%' FUNCTION f() RETURNS INT RETURN 1",
			want: "CREATE FUNCTION f() RETURNS INT RETURN 1",
		},
		{
			name: "current_user",
			in:   "CREATE DEFINER=CURRENT_USER EVENT ev ON SCHEDULE EVERY 1 DAY DO SET @a=1",
			want: "CREATE EVENT ev ON SCHEDULE EVERY 1 DAY DO SET @a=1",
		},
		{
			name: "current_user with parens",
			in:   "CREATE DEFINER = CURRENT_USER() EVENT ev ON SCHEDULE EVERY 1 DAY DO SET @a=1",
			want: "CREATE EVENT ev ON SCHEDULE EVERY 1 DAY DO SET @a=1",
		},
		{
			name: "spaces around equals",
			in:   "CREATE DEFINER = `u` @ `h` TRIGGER trg AFTER DELETE ON t FOR EACH ROW SET @a=1",
			want: "CREATE TRIGGER trg AFTER DELETE ON t FOR EACH ROW SET @a=1",
		},
		{
			name: "backtick with embedded doubled backtick",
			in:   "CREATE DEFINER=`we``ird`@`%` TRIGGER trg BEFORE UPDATE ON t FOR EACH ROW SET @a=1",
			want: "CREATE TRIGGER trg BEFORE UPDATE ON t FOR EACH ROW SET @a=1",
		},
		{
			name: "no definer clause unchanged",
			in:   "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET @a=1",
			want: "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET @a=1",
		},
		{
			name: "definer text inside body string is untouched",
			// The regex requires whitespace immediately before DEFINER; inside
			// the string literal the preceding byte is a quote, so no match.
			in:   "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET NEW.v = 'DEFINER=`x`@`y` stays'",
			want: "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET NEW.v = 'DEFINER=`x`@`y` stays'",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := StripDefiner(tc.in)
			if got != tc.want {
				t.Errorf("StripDefiner(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestStripDefinerKeepsRestOfStatement(t *testing.T) {
	in := "CREATE DEFINER=`u`@`h` PROCEDURE p(IN x INT)\nBEGIN\n  SELECT x;\nEND"
	got := StripDefiner(in)
	if !strings.Contains(got, "PROCEDURE p(IN x INT)") || !strings.Contains(got, "SELECT x;") {
		t.Errorf("body damaged: %q", got)
	}
	if strings.Contains(got, "DEFINER") {
		t.Errorf("DEFINER not removed: %q", got)
	}
}
