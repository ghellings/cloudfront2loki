package config

import (
	"io/ioutil"
	"os"
	"testing"

	"gopkg.in/yaml.v2"
)

func TestLoadConfig(t *testing.T) {
	testcfg := &Config{}
	testpath := createTempConfig("/promtail-cloudfront.conf", testcfg)
	defer os.RemoveAll(testpath)
	
	_,err := LoadConfig(testpath)
	if err != nil {
		t.Errorf("Expect no error, got: %s\n", err)
	}
}

func TestEnvOverrideConfig(t *testing.T) {
	testcfg := &Config{
		Default_Download_Concurrency: "2", 
	}
	testpath := createTempConfig("/promtail-cloudfront.conf", testcfg)
	defer os.RemoveAll(testpath)

	os.Setenv("DEFAULT_DOWNLOAD_CONCURRENCY", "1")
	defer os.Unsetenv("DEFAULT_DOWNLOAD_CONCURRENCY")

	config, err := LoadConfig(testpath)
	if err != nil {
		t.Errorf("Expected no error, got: %s\n", err)
	}
	if config.Default_Download_Concurrency != "1" {
		t.Errorf("Expected 'Default_Download_Concurrency' to equal 1, got: %s\n", config.Default_Download_Concurrency)
	}
}

func createTempConfig(cfgname string, cfg *Config ) (cfgpath string) {
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