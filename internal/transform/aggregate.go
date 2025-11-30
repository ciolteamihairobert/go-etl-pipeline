package transform

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func Aggregate(rows [][]string, header []string, groupBy string, operations map[string]string) ([]string, [][]string) { // functie pentru agregarea datelor
	logger.Info.Printf("Aggregation step started | group_by=%s", groupBy) // logam inceputul agregarii

	groupCol := indexOf(header, groupBy) // gasim indexul coloanei de grupare
	if groupCol == -1 {                  // daca coloana nu exista
		logger.Error.Printf("GroupBy column '%s' not found in header %v", groupBy, header) // logam eroarea
		panic(fmt.Sprintf("GroupBy column '%s' not found", groupBy))                       // panicam cu eroarea
	}

	buckets := map[string][][]string{} // map pentru a tine grupurile de randuri
	for _, r := range rows {           // iteram prin randuri
		key := r[groupCol]                     // obtinem cheia de grupare
		buckets[key] = append(buckets[key], r) // adaugam randul la grupul corespunzator
	}

	logger.Info.Printf("Aggregation groups: %d", len(buckets)) // logam numarul de grupuri formate

	newHeader := []string{groupBy}   // cream noul header cu coloana de grupare
	for opName := range operations { // iteram prin operatiile de agregare
		newHeader = append(newHeader, opName) // adaugam numele operatiei la header
	}

	var result [][]string // slice pentru a tine randurile agregate

	for key, bucket := range buckets { // iteram prin grupuri
		row := []string{key} // cream un nou rand cu cheia de grupare

		for outName, expr := range operations { // iteram prin operatii
			expr = strings.TrimSpace(expr) // curatam expresia

			if strings.HasPrefix(expr, "sum(") && strings.HasSuffix(expr, ")") { // SUM()
				colName := expr[4 : len(expr)-1]   // extragem numele coloanei
				colIdx := indexOf(header, colName) // gasim indexul coloanei
				if colIdx == -1 {                  // daca coloana nu exista
					logger.Error.Printf("SUM column '%s' not found", colName) // logam eroarea
					panic(fmt.Sprintf("SUM column '%s' not found", colName))  // panicam cu eroarea
				}

				var sum float64            // variabila pentru suma
				for _, r := range bucket { // iteram prin randurile din grup
					v, err := strconv.ParseFloat(r[colIdx], 64) // convertim valoarea la float
					if err != nil {                             // daca apare o eroare la conversie
						logger.Error.Printf("Invalid numeric value '%s' in column '%s'", r[colIdx], colName) // logam eroarea
						panic(fmt.Sprintf("Invalid number '%s' in SUM(%s)", r[colIdx], colName))             // panicam cu eroarea
					}
					sum += v // adaugam valoarea la suma
				}

				row = append(row, fmtFloat(sum)) // adaugam suma la rand
				continue
			}

			if expr == "count()" { // COUNT()
				row = append(row, strconv.Itoa(len(bucket))) // adaugam numarul de randuri la rand
				continue
			}

			logger.Error.Printf("Unknown aggregation expression '%s' for output '%s'", expr, outName) // logam eroarea
			panic(fmt.Sprintf("Unknown expression '%s' for '%s'", expr, outName))                     // panicam cu eroarea
		}

		result = append(result, row) // adaugam randul agregat la rezultat
	}

	logger.Info.Printf("Aggregation complete: %d final rows", len(result)) // logam numarul de randuri finale
	return newHeader, result                                               // returnam noul header si randurile agregate
}

func fmtFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64) // formatam float-ul ca string
}
