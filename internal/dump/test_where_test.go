package dump

import "testing"

func TestParseWhereCondition_Global(t *testing.T) {
	do := &DumpOptions{WhereConditions: make(map[string]string)}
	ParseWhereCondition("status = 'active'", do)

	if do.GlobalWhereCondition != "status = 'active'" {
		t.Errorf("expected GlobalWhereCondition \"status = 'active'\", got %q", do.GlobalWhereCondition)
	}
}

func TestParseWhereCondition_TableSpecific(t *testing.T) {
	do := &DumpOptions{WhereConditions: make(map[string]string)}
	ParseWhereCondition("users:age > 18,posts:published = 1", do)

	expected := map[string]string{
		"users": "age > 18",
		"posts": "published = 1",
	}
	for table, condition := range expected {
		if do.WhereConditions[table] != condition {
			t.Errorf("WhereConditions[%q]: expected %q, got %q", table, condition, do.WhereConditions[table])
		}
	}
}

func TestParseWhereCondition_GlobalWithTimeLiteral(t *testing.T) {
	// Regression: a global condition on a DATETIME/TIMESTAMP column contains
	// colons (from HH:MM:SS) that must not be mistaken for the table:condition
	// separator used by the per-table syntax.
	do := &DumpOptions{WhereConditions: make(map[string]string)}
	condition := "last_update >= '2026-06-01 00:00:00' AND last_update < '2026-07-01 00:00:00'"
	ParseWhereCondition(condition, do)

	if do.GlobalWhereCondition != condition {
		t.Errorf("expected GlobalWhereCondition %q, got %q", condition, do.GlobalWhereCondition)
	}
	if len(do.WhereConditions) != 0 {
		t.Errorf("expected no per-table conditions, got %#v", do.WhereConditions)
	}
}

func TestParseWhereCondition_TableSpecificWithTimeLiteral(t *testing.T) {
	// A legitimate per-table entry ("db.tbl:") must still work when its own
	// condition value contains colons.
	do := &DumpOptions{WhereConditions: make(map[string]string)}
	ParseWhereCondition("myapp.orders:last_update >= '2026-06-01 00:00:00'", do)

	expect := "last_update >= '2026-06-01 00:00:00'"
	if do.WhereConditions["`myapp`.`orders`"] != expect {
		t.Errorf("expected WhereConditions[`myapp`.`orders`] = %q, got %#v", expect, do.WhereConditions)
	}
	if do.GlobalWhereCondition != "" {
		t.Errorf("expected no global condition, got %q", do.GlobalWhereCondition)
	}
}
