package validation

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/config"
	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func ValidateRow(header []string, row []string, rules []config.ValidationRule) error { // functie pentru validarea unui rand
	indexMap := make(map[string]int) // map pentru indexarea coloanelor
	for i, h := range header {       // iteram prin header
		indexMap[h] = i // mapam numele coloanei la indexul sau
	}

	for _, rule := range rules { // iteram prin regulile de validare
		colIdx, ok := indexMap[rule.Field] // gasim indexul coloanei pentru field-ul din regula
		if !ok {                           // daca field-ul nu exista in header
			logger.Error.Printf("Validation failed: field '%s' not found in header", rule.Field) // logam eroarea
			return fmt.Errorf("validation error: field '%s' not found", rule.Field)              // returnam eroarea
		}

		val := row[colIdx]                // obtinem valoarea din rand pentru coloana respectiva
		r := strings.TrimSpace(rule.Rule) // eliminam spatiile albe din jurul regulii

		switch {
		case r == "not_empty": // daca regula este not_empty
			if val == "" { // daca valoarea este goala
				logger.Error.Printf("Validation failed: field '%s' is empty", rule.Field) // logam eroarea
				return fmt.Errorf("validation error: field '%s' is empty", rule.Field)    // returnam eroarea
			}

		case r == "numeric": // daca regula este numeric
			if _, err := strconv.ParseFloat(val, 64); err != nil { // incercam sa convertim valoarea la float
				logger.Error.Printf("Validation failed: field '%s' expected numeric but found '%s'",
					rule.Field, val) // logam eroarea
				return fmt.Errorf("validation error: field '%s' is not numeric: '%s'", rule.Field, val) // returnam eroarea
			}

		case strings.HasPrefix(r, "one_of:"): // daca regula este one_of
			allowedList := strings.TrimPrefix(r, "one_of:") // extragem lista de valori permise
			options := strings.Split(allowedList, ",")      // impartim lista in valori individuale

			ok := false                 // flag pentru a verifica daca valoarea este permisa
			for _, o := range options { // iteram prin valorile permise
				if strings.TrimSpace(o) == val { // daca valoarea curenta este permisa
					ok = true // setam flag-ul pe true
					break
				}
			}

			if !ok { // daca valoarea nu este permisa
				logger.Error.Printf("Validation failed: field '%s' value '%s' not allowed (allowed: %s)", rule.Field, val, allowedList) // logam eroarea
				return fmt.Errorf("validation error: field '%s' value '%s' not allowed [%s]", rule.Field, val, allowedList)             // returnam eroarea
			}

		default: // daca regula nu este recunoscuta
			logger.Error.Printf("Unknown validation rule '%s' for field '%s'", r, rule.Field) // logam eroarea
			return fmt.Errorf("unknown validation rule '%s' for field '%s'", r, rule.Field)   // returnam eroarea
		}
	}

	logger.Info.Println("Row passed validation.") // logam un mesaj de succes
	return nil                                    // returnam nil daca randul este valid
}
