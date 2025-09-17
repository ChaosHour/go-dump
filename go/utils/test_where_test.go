package utils

import (
	"strings"
	"testing"
)

func parseWhereCondition(whereValue string, dumpOptions *DumpOptions) {
	if strings.Contains(whereValue, ":") {
		// Table-specific: "table:condition,table2:condition2"
		parts := strings.Split(whereValue, ",")
		if dumpOptions.WhereConditions == nil {
			dumpOptions.WhereConditions = make(map[string]string)
		}
		for _, part := range parts {
			if tableCond := strings.SplitN(strings.TrimSpace(part), ":", 2); len(tableCond) == 2 {
				dumpOptions.WhereConditions[tableCond[0]] = tableCond[1]
			}
		}
	} else {
		// Global WHERE condition
		dumpOptions.GlobalWhereCondition = whereValue
	}
}

func TestParseWhereCondition_Global(t *testing.T) {
	dumpOptions := &DumpOptions{
		WhereConditions: make(map[string]string),
	}

	whereStr := "status = 'active'"
	parseWhereCondition(whereStr, dumpOptions)

	if dumpOptions.GlobalWhereCondition != "status = 'active'" {
		t.Errorf("Expected GlobalWhereCondition to be 'status = 'active'', got '%s'", dumpOptions.GlobalWhereCondition)
	}
}

func TestParseWhereCondition_TableSpecific(t *testing.T) {
	dumpOptions := &DumpOptions{
		WhereConditions: make(map[string]string),
	}

	whereStr := "users:age > 18,posts:published = 1"
	parseWhereCondition(whereStr, dumpOptions)

	expected := map[string]string{
		"users": "age > 18",
		"posts": "published = 1",
	}

	for table, condition := range expected {
		if dumpOptions.WhereConditions[table] != condition {
			t.Errorf("Expected WhereConditions[%s] to be '%s', got '%s'", table, condition, dumpOptions.WhereConditions[table])
		}
	}
}
