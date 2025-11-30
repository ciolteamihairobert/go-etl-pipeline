package transform

import "github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"

func ApplyMapping(rows [][]string, header []string, mapping map[string]string) ([]string, [][]string) { // functie pentru maparea coloanelor
	logger.Info.Printf("Applying mapping: %v", mapping) // logam mapping-ul aplicat

	newHeader := make([]string, len(header)) // cream un nou header
	copy(newHeader, header)                  // copiem header-ul vechi in noul header

	for oldName, newName := range mapping { // iteram prin mapping
		for i, h := range newHeader { // iteram prin header
			if h == oldName { // daca gasim coloana veche
				newHeader[i] = newName // inlocuim cu numele nou
			}
		}
	}

	logger.Info.Println("Mapping applied.") // logam un mesaj de succes
	return newHeader, rows                  // returnam noul header si randurile neschimbate
}
