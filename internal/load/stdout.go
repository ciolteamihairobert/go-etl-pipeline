package load

import (
	"fmt"
)

func ToStdout(header []string, rows [][]string) error { // functie pentru incarcarea datelor in stdout
	fmt.Println("OUTPUT:")   // afisam un mesaj de output
	fmt.Println(header)      // afisam header-ul
	for _, r := range rows { // iteram prin randuri
		fmt.Println(r) // afisam fiecare rand
	}
	return nil // returnam nil daca totul a decurs bine
}
