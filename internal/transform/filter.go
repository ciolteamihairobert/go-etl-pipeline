package transform

import (
	"strings"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func ApplyFilter(rows [][]string, header []string, expression string) [][]string {
	logger.Info.Printf("Applying filter: %s", expression) // logam expresia de filtrare

	field, value := parseExpression(expression) // parsam expresia

	col := indexOf(header, field) // gasim indexul coloanei
	if col == -1 {                // daca coloana nu exista
		logger.Error.Printf("Filter field '%s' not found", field) // logam o eroare
		return rows                                               // returnam randurile neschimbate
	}

	var out [][]string       // slice pentru a tine randurile filtrate
	for _, r := range rows { // iteram prin randuri
		if r[col] == value { // daca valoarea din coloana este egala cu valoarea cautata
			out = append(out, r) // adaugam randul la output
		}
	}

	logger.Info.Printf("Filter result: %d rows", len(out)) // logam numarul de randuri ramase dupa filtrare
	return out                                             // returnam randurile filtrate
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
