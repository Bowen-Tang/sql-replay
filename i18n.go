package main

import (
	"fmt"
	"sync"
)

// I18n 结构体定义
type I18n struct {
	translations map[string]map[string]string
	defaultLang  string
	mu           sync.RWMutex
}

// 修改后的 NewI18n 函数，使用 translations.go 中定义的 translations 变量
func NewI18n(defaultLang string) (*I18n, error) {
	i18n := &I18n{
		translations: translations, // 使用 translations.go 中的 translations 变量
		defaultLang:  defaultLang,
	}
	return i18n, nil
}

// T 方法用于获取翻译并格式化字符串
func (i *I18n) T(lang, key string, args ...interface{}) string {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var message string

	// 尝试从指定语言中获取翻译
	if langMessages, ok := i.translations[lang]; ok {
		if msg, ok := langMessages[key]; ok {
			message = msg
		}
	}

	// 如果指定语言没有找到，尝试使用默认语言
	if message == "" {
		if langMessages, ok := i.translations[i.defaultLang]; ok {
			if msg, ok := langMessages[key]; ok {
				message = msg
			}
		}
	}

	// 如果仍然找不到翻译，直接返回键值
	if message == "" {
		return key
	}

	// 如果没有参数，直接返回消息模板
	if len(args) == 0 {
		return message
	}

	// 使用参数格式化消息模板
	return fmt.Sprintf(message, args...)
}
