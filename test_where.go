package main

import (
	"fmt"
	"strings"

	"github.com/martinarrieta/go-dump/go/utils"
)

func parseWhereCondition(whereValue string, dumpOptions *utils.DumpOptions) {
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

func main() {
	// Test WHERE parsing
	dumpOptions := &utils.DumpOptions{
		WhereConditions: make(map[string]string),
	}

	// Test global WHERE
	whereStr := "status = 'active'"
	parseWhereCondition(whereStr, dumpOptions)
	fmt.Printf("Global WHERE: %s\n", dumpOptions.GlobalWhereCondition)

	// Test table-specific WHERE
	dumpOptions.WhereConditions = make(map[string]string)
	whereStr = "users:age > 18,posts:published = 1"
	parseWhereCondition(whereStr, dumpOptions)
	fmt.Printf("Table-specific WHERE:\n")
	for table, condition := range dumpOptions.WhereConditions {
		fmt.Printf("  %s: %s\n", table, condition)
	}
}
