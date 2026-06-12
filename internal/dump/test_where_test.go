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
