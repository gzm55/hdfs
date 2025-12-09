// Package hadoopconf provides utilities for reading and parsing Hadoop's xml
// configuration files.
package hadoopconf

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type propertyList struct {
	Property []property `xml:"property"`
}

var confFiles = []string{"core-site.xml", "hdfs-site.xml", "mapred-site.xml"}

// HadoopConf represents a map of all the key value configutation
// pairs found in a user's hadoop configuration files.
type HadoopConf map[string]string

// LoadFromEnvironment tries to locate the Hadoop configuration files based on
// the environment, and returns a HadoopConf object representing the parsed
// configuration. If the HADOOP_CONF_DIR environment variable is specified, it
// uses that, or if HADOOP_HOME is specified, it uses $HADOOP_HOME/conf.
//
// If no configuration can be found, it returns a nil map. If the configuration
// files exist but there was an error opening or parsing them, that is returned
// as well.
func LoadFromEnvironment() (HadoopConf, error) {
	hadoopConfDir := os.Getenv("HADOOP_CONF_DIR")
	if hadoopConfDir != "" {
		if conf, err := Load(hadoopConfDir); conf != nil || err != nil {
			return conf, err
		}
	}

	hadoopHome := os.Getenv("HADOOP_HOME")
	if hadoopHome != "" {
		if conf, err := Load(filepath.Join(hadoopHome, "conf")); conf != nil || err != nil {
			return conf, err
		}
	}

	return nil, nil
}

// Load returns a HadoopConf object representing configuration from the
// specified path. It will parse core-site.xml, hdfs-site.xml, and
// mapred-site.xml.
//
// If no configuration files could be found, Load returns a nil map. If the
// configuration files exist but there was an error opening or parsing them,
// that is returned as well.
func Load(path string) (HadoopConf, error) {
	var conf HadoopConf

	for _, file := range confFiles {
		pList := propertyList{}
		f, err := ioutil.ReadFile(filepath.Join(path, file))
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return conf, err
		}

		err = xml.Unmarshal(f, &pList)
		if err != nil {
			return conf, fmt.Errorf("%s: %s", path, err)
		}

		if conf == nil {
			conf = make(HadoopConf)
		}

		for _, prop := range pList.Property {
			conf[prop.Name] = prop.Value
		}
	}

	return conf, nil
}

// Namenodes returns the default namenode hosts present in the configuration. The
// returned slice will be sorted and deduped. The values are loaded from
// fs.defaultFS (or the deprecated fs.default.name), or fields beginning with
// dfs.namenode.rpc-address.
//
// To handle 'logical' clusters Namenodes will not return any cluster names
// found in dfs.ha.namenodes.<clustername> properties.
//
// If no namenode addresses can befound, Namenodes returns a nil slice.
// This function only works for old single NameService Hadoop conf
// It should be deprecated
func (conf HadoopConf) Namenodes() []string {
	defNSID := conf.DefaultNSID()
	if defNSID != "" {
		return conf.AddressesByNameServiceID(defNSID)
	}

	// fallback to pick up all namenodex in XML

	nns := make(map[string]bool)
	var clusterNames []string

	for key, value := range conf {
		if strings.HasPrefix(key, "fs.default") {
			nnUrl, _ := url.Parse(value)
			nns[nnUrl.Host] = true
		} else if strings.HasPrefix(key, "dfs.namenode.rpc-address.") {
			nns[value] = true
		} else if strings.HasPrefix(key, "dfs.ha.namenodes.") {
			clusterNames = append(clusterNames, key[len("dfs.ha.namenodes."):])
		}
	}

	for _, cn := range clusterNames {
		delete(nns, cn)
	}

	if len(nns) == 0 {
		return nil
	}

	keys := make([]string, 0, len(nns))
	for k, _ := range nns {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	return keys
}

// return the NameServiceID of defaultFS
func (conf HadoopConf) DefaultNSID() string {
	value := conf.DefaultFS()
	if strings.HasPrefix(value, "viewfs://") {
		return value;
	}
	if strings.HasPrefix(value, "hdfs://") {
		nnUrl, _ := url.Parse(value)
		return nnUrl.Host
	}
	return ""
}

// return the defaultFS
func (conf HadoopConf) DefaultFS() string {
	value, _ := conf["fs.defaultFS"]
	if value == "" { // fallback to deprecated form
		value, _ = conf["fs.default.name"]
	}
	return value
}

// return the HA Address of namenode
func (conf HadoopConf) AddressesByNameServiceID(nsid string) []string {
	rets := make([]string, 0, 8)
	// for viewfs:// case and very simple host:port
	if strings.HasPrefix(nsid, "viewfs://") || strings.Contains(nsid, ":") {
		return []string{nsid}
	}

	// for simple
	key := "dfs.namenode.rpc-address." + nsid
	addr, ok := conf[key]
	if ok {
		rets = append(rets, addr)
		return rets
	}
	// for HA
	haListName := "dfs.ha.namenodes." + nsid
	haListStr, ok := conf[haListName]
	var haList []string
	haList = strings.Split(haListStr, ",")
	for _, haName := range haList {
		key := "dfs.namenode.rpc-address." + nsid + "." + haName
		addr, ok := conf[key]
		if ok && addr != "" {
			rets = append(rets, addr)
		}
	}
	// sort and return
	if len(rets) <= 0 {
		//return nil
		return []string{"111"}
	} else {
		sort.Strings(rets)
		return rets
	}
}

var errInvalidHDFSFilename = errors.New("invalid HDFS Filename")

// return the actual path on the final namenode of a viewfs path
// if property
//   fs.viewfs.mounttable.nsX.link./user = hdfs://SunshineNameNode3/user2
//   defaultFS = nsX
// then
//  call ("/user/sub") returns ("SunshineNameNode3", "/user/sub", nil)
//  call ("viewfs://nsX/user/sub") returns ("SunshineNameNode3", "/user2/sub", nil)
func (conf HadoopConf) ViewfsReparseFilename(rootnsid string, filename string) (string, string, error) {
	var nsid, path string
	u, err := url.Parse(filename)
	if err != nil || (u.Scheme != "viewfs" && u.Scheme != "") {
		return "", "", errInvalidHDFSFilename
	}
	if u.Host != "" && rootnsid != "" { // host and nsid conflict
		return "", "", errInvalidHDFSFilename
	}
	nsid, path = u.Host, u.Path
	if u.Host == "" {
		nsid = rootnsid
	}
	if nsid == "" {
		nsid = conf.DefaultNSID()
	}

	nsid = strings.TrimPrefix(nsid, "viewfs://")

	dirs := strings.Split(path, "/")
	if dirs[0] != "" {
		dirs = append([]string{""}, dirs...)
	}
	for i := len(dirs); i > 0; i-- {
		prefix := strings.Join(dirs[0:i], "/")
		key := "fs.viewfs.mounttable." + nsid + ".link." + prefix
		value, ok := conf[key]
		if ok {
			postfix := filepath.Join(dirs[i:]...)
			newurl := value + "/" + postfix
			u, _ = url.Parse(newurl)
			return u.Host, u.Path, nil
		}
	}
	return "viewfs://" + nsid, path, nil
}

// type of namenode address string
type TypeOfNamenodeAddressString int

const (
	_ TypeOfNamenodeAddressString = iota
	TNAS_SimpleAddress
	TNAS_SimpleNameServiceID
	TNAS_ViewfsNameServiceID
)

// returns the type of namenode/address
// 3 situation
// A. begin with viewfs://
// A. Just a simple hostname (with port)
// C. A NameServiceID
// NOTE: mounttable link is not checked
func (conf HadoopConf) CheckTypeOfNameAddressString(maybe_addr string) TypeOfNamenodeAddressString {
	// viewfs
	if strings.HasPrefix(maybe_addr, "viewfs://") {
		return TNAS_ViewfsNameServiceID
	}

	// if address is "Host:Port" style
	if strings.Contains(maybe_addr, ":") {
		return TNAS_SimpleAddress
	}

	// check is it a NameServiceID
	nsids_str, _ := conf["dfs.nameservices"]
	nsids := strings.Split(nsids_str, ",")
	for _, v := range nsids {
		if maybe_addr == v {
			return TNAS_SimpleNameServiceID
		}
	}
	// check is there any HA nodes
	if _, ok := conf["dfs.ha.namenodes."+maybe_addr]; ok {
		return TNAS_SimpleNameServiceID
	}
	// fallback to simple address
	return TNAS_SimpleAddress
}
