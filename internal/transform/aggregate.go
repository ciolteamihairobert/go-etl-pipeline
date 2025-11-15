package transform

import (
	"fmt"
	"strconv"
	"strings"
)

func Aggregate(rows [][]string, header []string, groupBy string, operations map[string]string) ([]string, [][]string) { // functie pentru agregarea datelor
	groupCol := indexOf(header, groupBy) // obtinem indexul coloanei de grupare

	if groupCol == -1 { // daca coloana de grupare nu exista
		panic(fmt.Sprintf("GroupBy column '%s' not found in header %v", groupBy, header)) // panic cu un mesaj de eroare
	}

	buckets := map[string][][]string{} // cream un map pentru a tine grupurile
	for _, r := range rows {           // iteram prin randuri
		key := r[groupCol]                     // obtinem cheia de grupare
		buckets[key] = append(buckets[key], r) // adaugam randul in grupul corespunzator
	}

	newHeader := []string{groupBy}   // cream un nou header cu coloana de grupare
	for opName := range operations { // iteram prin operatiuni
		newHeader = append(newHeader, opName) // adaugam numele operatiunii in noul header
	}

	var result [][]string // cream un slice pentru rezultatele agregate

	for key, bucket := range buckets { // iteram prin fiecare grup
		row := []string{key} // incepem un nou rand cu cheia de grupare

		for outName, expr := range operations { // iteram prin operatiuni
			expr = strings.TrimSpace(expr) // eliminam spatiile albe

			if expr == "count()" { // daca expresia este count()
				row = append(row, strconv.Itoa(len(bucket))) // adaugam numarul de randuri din grup
				continue                                     // trecem la urmatoarea operatiune
			}

			if strings.HasPrefix(expr, "sum(") && strings.HasSuffix(expr, ")") { // daca expresia este sum(x)
				colName := expr[4 : len(expr)-1] // extragem numele coloanei

				colIdx := indexOf(header, colName) // obtinem indexul coloanei
				if colIdx == -1 {                  // daca coloana nu exista
					panic(fmt.Sprintf("Column '%s' in SUM not found in header %v", colName, header)) // panic cu un mesaj de eroare
				}

				var sum float64            // cream o variabila pentru suma
				for _, r := range bucket { // iteram prin randurile din grup
					v, err := strconv.ParseFloat(r[colIdx], 64) // convertim valoarea la float
					if err != nil {                             // daca apare o eroare la conversie
						panic(fmt.Sprintf("Invalid number '%s' for column '%s'", r[colIdx], colName)) // panic cu un mesaj de eroare
					}
					sum += v // adaugam valoarea la suma
				}
				row = append(row, fmtFloat(sum)) // adaugam suma la rand
				continue
			}

			panic(fmt.Sprintf("Unknown aggregate expression '%s' for field '%s'", expr, outName)) // panic pentru expresii necunoscute
		}

		result = append(result, row) // adaugam randul la rezultate
	}

	return newHeader, result // returnam noul header si rezultatele agregate
}

func fmtFloat(f float64) string { // functie pentru formatarea unui float ca string
	return strconv.FormatFloat(f, 'f', -1, 64) // convertim float-ul la string
}
