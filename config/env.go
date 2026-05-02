package config

import (
	"os"
	"strings"
)

// 环境变量键名常量
const (
	EnvAllowedHosts = "MVZ_ALLOWED_HOSTS"
)

// GetEnvOrDefault 获取环境变量或返回默认值
func GetEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// GetAllowedHosts 获取允许的域名列表
func GetAllowedHosts() []string {
	hosts := GetEnvOrDefault(EnvAllowedHosts, "scarletborder.cn")
	return strings.Split(hosts, ",")
}
