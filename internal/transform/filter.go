package transform

import "strings"

func ApplyFilter(rows [][]string, header []string, expression string) [][]string {
	field, value := parseExpression(expression)

	col := indexOf(header, field)
	if col == -1 {
		return rows
	}

	var out [][]string
	for _, r := range rows {
		if r[col] == value {
			out = append(out, r)
		}
	}
	return out
}

func parseExpression(expr string) (string, string) {
	parts := strings.Split(expr, "==")
	field := strings.TrimSpace(parts[0])
	value := strings.Trim(strings.TrimSpace(parts[1]), "'")
	return field, value
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}
