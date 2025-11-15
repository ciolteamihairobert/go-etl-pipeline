package transform

func ApplyMapping(rows [][]string, header []string, mapping map[string]string) ([]string, [][]string) { // functie pentru aplicarea maparii pe date
	newHeader := make([]string, len(header)) // cream un nou header
	copy(newHeader, header)                  // copiem header-ul original in noul header

	for oldName, newName := range mapping { // iteram prin mapare
		for i, h := range newHeader { // iteram prin header
			if h == oldName { // daca gasim o potrivire
				newHeader[i] = newName // actualizam numele coloanei in noul header
			}
		}
	}

	return newHeader, rows // returnam noul header si randurile neschimbate
}
