package connector

import (
	"encoding/csv"
	"fmt"
	"os"
)

func ReadCSV(path string) error { // citim un fisier CSV de la calea specificata
	file, err := os.Open(path) // deschidem fisierul CSV
	if err != nil {            // daca apare o eroare la deschidere
		return err // returnam eroarea
	}
	defer file.Close() // inchidem fisierul la finalul functiei

	reader := csv.NewReader(file) // cream un cititor CSV
	rows, err := reader.ReadAll() // citim toate randurile din fisier
	if err != nil {               // daca apare o eroare la citire
		return err // returnam eroarea
	}

	fmt.Println("CSV Rows:")   // afisam randurile citite
	for i, row := range rows { // iteram prin fiecare rand
		fmt.Printf("%d: %v\n", i, row) // afisam indexul si continutul randului
	}

	return nil // returnam nil daca totul a decurs bine
}
