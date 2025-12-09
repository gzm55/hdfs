package hadoopconf

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfFallback(t *testing.T) {
	oldHome := os.Getenv("HADOOP_HOME")
	oldConfDir := os.Getenv("HADOOP_CONF_DIR")
	os.Setenv("HADOOP_HOME", "testdata") // This will resolve to testdata/conf.
	os.Setenv("HADOOP_CONF_DIR", "testdata/conf2")

	confNamenodes := []string{"namenode1:8020", "namenode2:8020"}
	conf2Namenodes := []string{"namenode3:8020"}

	conf, err := LoadFromEnvironment()
	assert.NoError(t, err)

	nns := conf.Namenodes()
	assert.NoError(t, err)
	assert.EqualValues(t, conf2Namenodes, nns, "loading via HADOOP_CONF_DIR (testdata/conf2)")

	os.Unsetenv("HADOOP_CONF_DIR")

	conf, err = LoadFromEnvironment()
	assert.NoError(t, err)

	nns = conf.Namenodes()
	assert.NoError(t, err)
	assert.EqualValues(t, confNamenodes, nns, "loading via HADOOP_HOME (testdata/conf)")

	os.Setenv("HADOOP_HOME", oldHome)
	os.Setenv("HADOOP_CONF_DIR", oldConfDir)
}

func TestConfCheckTypeOfNameAddressString(t *testing.T) {
	var ty TypeOfNamenodeAddressString

	oldHome := os.Getenv("HADOOP_HOME")
	oldConfDir := os.Getenv("HADOOP_CONF_DIR")
	os.Setenv("HADOOP_HOME", "testdata") // This will resolve to testdata/conf.
	os.Setenv("HADOOP_CONF_DIR", "testdata/conf-viewfs")

	conf, err := LoadFromEnvironment()
	assert.NoError(t, err)

	ty = conf.CheckTypeOfNameAddressString("nsX")
	assert.EqualValues(t, TNAS_ViewfsNameServiceID, ty, "check addr type in (test/conf-viewfs)")

	ty = conf.CheckTypeOfNameAddressString("nsY")
	assert.EqualValues(t, TNAS_ViewfsNameServiceID, ty, "check addr type in (test/conf-viewfs)")

	ty = conf.CheckTypeOfNameAddressString("localhost:8080")
	assert.EqualValues(t, TNAS_SimpleAddress, ty, "check addr type in (test/conf-viewfs)")

	ty = conf.CheckTypeOfNameAddressString("SunshineNameNode1")
	assert.EqualValues(t, TNAS_SimpleNameServiceID, ty, "check addr type in (test/conf-viewfs)")

	os.Setenv("HADOOP_HOME", oldHome)
	os.Setenv("HADOOP_CONF_DIR", oldConfDir)
}

func TestConfWithViewfs(t *testing.T) {
	var nns []string
	var err error
	var newnsid, newpath string

	oldHome := os.Getenv("HADOOP_HOME")
	oldConfDir := os.Getenv("HADOOP_CONF_DIR")
	os.Setenv("HADOOP_HOME", "testdata") // This will resolve to testdata/conf.
	os.Setenv("HADOOP_CONF_DIR", "testdata/conf-viewfs")

	snn2Addrs := []string{"localhost:9000", "localhost:9001"}

	conf, err := LoadFromEnvironment()
	assert.NoError(t, err)

	defNsid := conf.DefaultNSID()
	assert.EqualValues(t, "viewfs://nsX", defNsid, "check defaultNSID in (test/conf-viewfs)")

	nns = conf.Namenodes()
	assert.EqualValues(t, []string{"viewfs://nsX"}, nns, "loading via specified path (test/conf-viewfs)")

	nns = conf.AddressesByNameServiceID("SunshineNameNode2")
	assert.EqualValues(t, snn2Addrs, nns, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("viewfs://nsX", "/norm")
	assert.Nil(t, err)
	assert.EqualValues(t, "viewfs://nsX", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/norm", newpath, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("", "/norm")
	assert.Nil(t, err)
	assert.EqualValues(t, "viewfs://nsX", newnsid, "loading via specified path and default nsid (test/conf-viewfs)")
	assert.EqualValues(t, "/norm", newpath, "loading via specified path and default nsid (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("", "hdfs://nsX/cloud/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "SunshineNameNode1", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/_cloud/sub", newpath, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("viewfs://nsX", "/user/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "SunshineNameNode2", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/_user/sub", newpath, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("", "/user/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "SunshineNameNode2", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/_user/sub", newpath, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("nsY", "/app/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "SunshineNameNode3", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/_app/sub", newpath, "loading via specified path (test/conf-viewfs)")

	newnsid, newpath, err = conf.ViewfsReparseFilename("", "hdfs://nsZ/app/sub")
	assert.Nil(t, err)
	assert.EqualValues(t, "nsZ", newnsid, "loading via specified path (test/conf-viewfs)")
	assert.EqualValues(t, "/app/sub", newpath, "loading via specified path (test/conf-viewfs)")

	os.Setenv("HADOOP_HOME", oldHome)
	os.Setenv("HADOOP_CONF_DIR", oldConfDir)
}
