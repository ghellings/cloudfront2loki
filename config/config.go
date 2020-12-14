package config

import (

    "github.com/spf13/viper"
)

type Config struct {
    Region          string
    Bucket          string
    Prefix          string
    Concurrency     string
    LokiHost        string
}

func LoadConfig(path string) (config Config, err error) {
    viper.SetConfigName("promtail-cloudfront.conf")
    viper.SetConfigType("yaml")
    viper.AddConfigPath(path)
    viper.AddConfigPath("/etc/promtail-cloudfront")
    viper.AddConfigPath("/app/promtail-cloudfront")
    viper.AddConfigPath(".")
    viper.AutomaticEnv()
    err = viper.ReadInConfig()
    if err != nil {
        return
    }

    err = viper.Unmarshal(&config)
    return
}