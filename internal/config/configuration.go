package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type PipelineConfig struct {
	Name           string           `yaml:"name"` //numele pipeline-ului (in cazul asta e sales-aggregator)
	Extract        ExtractConfig    `yaml:"extract"`
	Transform      []TransformStep  `yaml:"transform"`
	Load           LoadConfig       `yaml:"load"`
	Schedule       ScheduleConfig   `yaml:"schedule,omitempty"`
	DataValidation []ValidationRule `yaml:"validate,omitempty"`
}

type ExtractConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

type TransformStep struct {
	Type       string            `yaml:"type"`
	Mapping    map[string]string `yaml:"mapping,omitempty"`
	Expression string            `yaml:"expression,omitempty"`
	GroupBy    string            `yaml:"group_by,omitempty"`
	Operations map[string]string `yaml:"operations,omitempty"`
}

type LoadConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"` // un map generic cu key string si values ce se vrea
	// pentru configuratii specifice fiecarui tip de load
}

type ScheduleConfig struct {
	IntervalSeconds int `yaml:"interval_seconds"` // intervalul la care se ruleaza pipeline-ul
	Retries         int `yaml:"retries"`          // numarul de retries
}

type ValidationRule struct {
	Field string `yaml:"field"` // numele coloanei (ex: "amount")
	Rule  string `yaml:"rule"`  // regula (ex: "numeric", "not_empty", "one_of:Completed,Pending")
}

func LoadPipelineConfig(path string) (*PipelineConfig, error) { // dam ca parametru calea catre fisierul yaml,
	//  returneaza un pointer catre un PipelineConfig struct si o eroare
	data, err := os.ReadFile(path) // citim continutul fisierului daca exista
	if err != nil {                // daca apare o eroare la citire
		return nil, err // returnam nil si eroarea
	}

	var cfg PipelineConfig           // cream o instanta a structurii PipelineConfig
	err = yaml.Unmarshal(data, &cfg) // deserializam datele yaml in structura cfg
	if err != nil {                  // daca apare o eroare la deserializare
		return nil, err // returnam nil si eroarea
	}

	return &cfg, nil // returnam un pointer catre structura cfg si nil pentru eroare
}
