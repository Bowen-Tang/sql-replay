package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type I18n struct {
	translations map[string]map[string]string
	defaultLang  string
	mu           sync.RWMutex
}

func NewI18n(filePath string, defaultLang string) (*I18n, error) {
	i18n := &I18n{
		defaultLang: defaultLang,
	}

	err := i18n.loadTranslations(filePath)
	if err != nil {
		return nil, err
	}

	return i18n, nil
}

func (i *I18n) loadTranslations(filePath string) error {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(file, &i.translations)
	if err != nil {
		return err
	}

	return nil
}

func (i *I18n) T(lang, key string, args ...interface{}) string {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if langMessages, ok := i.translations[lang]; ok {
		if message, ok := langMessages[key]; ok {
			return fmt.Sprintf(message, args...)
		}
	}

	if langMessages, ok := i.translations[i.defaultLang]; ok {
		if message, ok := langMessages[key]; ok {
			return fmt.Sprintf(message, args...)
		}
	}

	return key
}
