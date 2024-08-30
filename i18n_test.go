package main

import (
	"testing"
)

// 测试 NewI18n
func TestNewI18n(t *testing.T) {
	// Test NewI18n
	i18n, err := NewI18n("en")
	if err != nil {
		t.Fatalf("NewI18n failed: %v", err)
	}

	if i18n.defaultLang != "en" {
		t.Errorf("Expected default language 'en', got '%s'", i18n.defaultLang)
	}

	if len(i18n.translations) != 2 {
		t.Errorf("Expected 2 languages, got %d", len(i18n.translations))
	}
}

// 测试 T 方法
func TestI18n_T(t *testing.T) {
	i18n := &I18n{
		translations: map[string]map[string]string{
			"en": {
				"hello":     "Hello",
				"world":     "World",
				"with_args": "Hello, %s!",
			},
			"es": {
				"hello":     "Hola",
				"world":     "Mundo",
				"with_args": "¡Hola, %s!",
			},
		},
		defaultLang: "en",
	}

	tests := []struct {
		lang     string
		key      string
		args     []interface{}
		expected string
	}{
		{"en", "hello", nil, "Hello"},
		{"es", "hello", nil, "Hola"},
		{"en", "world", nil, "World"},
		{"es", "world", nil, "Mundo"},
		{"fr", "hello", nil, "Hello"},  // Fallback to default language
		{"en", "unknown", nil, "unknown"}, // Key not found
		{"en", "with_args", []interface{}{"John"}, "Hello, John!"},
		{"es", "with_args", []interface{}{"Juan"}, "¡Hola, Juan!"},
	}

	for _, tt := range tests {
		t.Run(tt.lang+"/"+tt.key, func(t *testing.T) {
			result := i18n.T(tt.lang, tt.key, tt.args...)
			if result != tt.expected {
				t.Errorf("T(%q, %q, %v) = %q, want %q", tt.lang, tt.key, tt.args, result, tt.expected)
			}
		})
	}
}
