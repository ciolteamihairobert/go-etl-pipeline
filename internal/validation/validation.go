package validation

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/config"
)

// ValidateRow valideaza un rand pe baza regulilor din config.Validate
// intoarce eroare la prima regula incalcata
func ValidateRow(header []string, row []string, rules []config.ValidationRule) error {
	// map pentru a gasi indexul unei coloane dupa nume
	indexMap := make(map[string]int)
	for i, h := range header {
		indexMap[h] = i
	}

	for _, rule := range rules { // iteram prin regulile de validare
		colIdx, ok := indexMap[rule.Field] // gasim indexul coloanei dupa nume
		if !ok {                           // daca coloana nu exista in header
			return fmt.Errorf("validation error: field '%s' not found in header", rule.Field) // returnam o eroare
		}
		val := row[colIdx] // obtinem valoarea din rand pentru coloana respectiva

		r := strings.TrimSpace(rule.Rule) // eliminam spatiile albe din jurul regulii

		switch {
		case r == "not_empty": // daca regula este not_empty
			if val == "" { // daca valoarea este goala
				return fmt.Errorf("validation error: field '%s' is empty", rule.Field) // returnam o eroare
			}

		case r == "numeric": // daca regula este numeric
			if _, err := strconv.ParseFloat(val, 64); err != nil { // incercam sa convertim valoarea la float
				return fmt.Errorf("validation error: field '%s' is not numeric: '%s'", rule.Field, val) // returnam o eroare
			}

		case strings.HasPrefix(r, "one_of:"): // daca regula incepe cu one_of:
			list := strings.TrimPrefix(r, "one_of:") // extragem lista de valori permise
			options := strings.Split(list, ",")      // impartim lista in valori individuale
			found := false                           // flag pentru a verifica daca valoarea este in lista
			for _, opt := range options {            // iteram prin optiuni
				if val == strings.TrimSpace(opt) { // daca valoarea se potriveste cu optiunea curenta
					found = true // setam flag-ul pe true
					break
				}
			}
			if !found { // daca valoarea nu a fost gasita in lista
				return fmt.Errorf("validation error: field '%s' value '%s' not in allowed set [%s]", rule.Field, val, list) // returnam o eroare
			}

		default:
			return fmt.Errorf("unknown validation rule '%s' for field '%s'", r, rule.Field) // returnam o eroare pentru regula necunoscuta
		}
	}

	return nil
}
