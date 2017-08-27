package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
)

var (
	configFilePath = "config.json"
)

// Configuration options
type Configuration struct {
	ClientID      string   `json:"ClientID" `
	ProducerURL   []string `json:"KafkaBrokers" `
	GroupID       string   `json:"KafkaConsumerGroup" `
	KafkaTopics   string   `json:"KafkaTopics" `
	FileName      string   `json:"OutputFilePath"`
	LogFile       string   `json:"LogFilePath"     `
	MaxProcessors int      `json:"MaxProcessors"  `
	Offset        int64    `json:"Offset"   `
}

//LoadConfiguration reads and loads configuration to Config variable
func LoadConfiguration() (Configuration, error) {
	return readConfigFromJSON(configFilePath)
}

// isMissing validates Configuration
func (c *Configuration) isMissing() bool {
	switch {
	case c.ClientID == "":
		return true
	case len(c.ProducerURL) == 0:
		return true
	case c.FileName == "":
		return true
	case len(c.KafkaTopics) == 0:
		return true
	default:
		return false
	}
}

func readConfigFromJSON(configFilePath string) (Configuration, error) {
	log.Printf("Looking for JSON config file in: %s\n", configFilePath)
	config := Configuration{}
	contents, err := ioutil.ReadFile(configFilePath)
	if err == nil {
		reader := bytes.NewBuffer(contents)
		err = json.NewDecoder(reader).Decode(&config)
	}
	if err != nil {
		log.Printf("Reading configuration from JSON (%s) failed: %s\n", configFilePath, err)
	}

	return config, err
}
