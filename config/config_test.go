package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestLoadConfig(t *testing.T) {
	testcfg := &Config{}
	testpath := createTempConfig("/cloudfront2loki.conf", testcfg)
	defer os.RemoveAll(testpath)

	_, err := LoadConfig(testpath)
	require.NoError(t, err)
}

func TestEnvOverrideConfig(t *testing.T) {
	testcfg := &Config{
		Concurrency: "2",
	}
	testpath := createTempConfig("/cloudfront2loki.conf", testcfg)
	defer os.RemoveAll(testpath)

	os.Setenv("CONCURRENCY", "1")
	defer os.Unsetenv("CONCURRENCY")

	config, err := LoadConfig(testpath)
	require.NoError(t, err)
	require.Equal(t, "1", config.Concurrency, fmt.Sprintf("Expected 'Concurrency' to equal 1, got: %s\n", config.Concurrency))
}

func createTempConfig(cfgname string, cfg *Config) (cfgpath string) {
	cfgyaml, err := yaml.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	cfgpath, err = ioutil.TempDir("", "promtail-cloudfront")
	if err != nil {
		panic(err)
	}
	cfgfullpath := cfgpath + "/" + cfgname
	err = ioutil.WriteFile(cfgfullpath, cfgyaml, 0644)
	if err != nil {
		panic(err)
	}
	return
}
