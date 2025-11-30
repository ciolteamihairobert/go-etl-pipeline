package connector

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func ExtractAPI(cfg map[string]interface{}) ([]string, [][]string, error) { // functie pentru extragerea datelor dintr-un API
	url := cfg["url"].(string)                          // obtinem URL-ul din configuratie
	logger.Info.Printf("Calling API endpoint: %s", url) // logam URL-ul apelat

	resp, err := http.Get(url) // facem cererea GET catre API
	if err != nil {            // daca apare o eroare la cerere
		logger.Error.Printf("API request failed: %v", err)         // logam eroarea
		return nil, nil, fmt.Errorf("api request failed: %w", err) // returnam eroarea
	}
	defer resp.Body.Close() // inchidem corpul raspunsului la final

	if resp.StatusCode >= 400 { // daca statusul HTTP este de eroare
		logger.Error.Printf("API returned HTTP %d", resp.StatusCode) // logam statusul
		return nil, nil, fmt.Errorf("api error %d", resp.StatusCode) // returnam eroarea
	}

	body, _ := ioutil.ReadAll(resp.Body) // citim corpul raspunsului

	var items []map[string]interface{}                   // slice pentru a tine obiectele JSON
	if err := json.Unmarshal(body, &items); err != nil { // deserializam JSON-ul
		logger.Error.Printf("Invalid JSON returned by API: %v", err) // logam eroarea
		return nil, nil, fmt.Errorf("invalid API JSON: %w", err)     // returnam eroarea
	}

	if len(items) == 0 { // daca lista este goala
		logger.Error.Printf("API returned empty list") // logam un mesaj de eroare
		return nil, nil, fmt.Errorf("api empty list")  // returnam eroarea
	}

	header := make([]string, 0, len(items[0])) // header-ul va fi cheile primului obiect
	for k := range items[0] {                  // iteram prin chei
		header = append(header, k) // adaugam cheia la header
	}

	var rows [][]string         // slice pentru a tine randurile
	for _, obj := range items { // iteram prin obiecte
		row := make([]string, 0, len(header)) // cream un nou rand
		for _, h := range header {            // iteram prin header
			row = append(row, fmt.Sprintf("%v", obj[h])) // adaugam valoarea corespunzatoare la rand
		}
		rows = append(rows, row) // adaugam randul la lista de randuri
	}

	logger.Info.Printf("API extracted %d rows successfully", len(rows)) // logam numarul de randuri extrase
	return header, rows, nil                                            // returnam header-ul, randurile si nil pentru eroare
}
