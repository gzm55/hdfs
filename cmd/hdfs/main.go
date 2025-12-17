package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"strings"
	"time"
	"sync"

	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	"github.com/pborman/getopt"
	krb "github.com/jcmturner/gokrb5/v8/client"
)

// TODO: cp, tree, trash

var (
	version string
	usage   = fmt.Sprintf(`Usage: %s COMMAND
The flags available are a subset of the POSIX ones, but should behave similarly.

Valid commands:
  ls [-lahR] [FILE]...
  rm [-rf] [--skipTrash] [--forceTrash] FILE...
  mv [-nT] SOURCE... DEST
  mkdir [-p] FILE...
  touch [-c] FILE...
  chmod [-R] OCTAL-MODE FILE...
  chown [-R] OWNER[:GROUP] FILE...
  cat SOURCE...
  head [-n LINES | -c BYTES] SOURCE...
  tail [-n LINES | -c BYTES] SOURCE...
  test [-defsz] FILE...
  du [-sh] FILE...
  checksum FILE...
  get SOURCE [DEST]
  getmerge SOURCE DEST
  put SOURCE DEST
  df [-h]
  setrep REP FILE...
  truncate SIZE FILE
`, os.Args[0])

	lsOpts = getopt.New()
	lsl    = lsOpts.Bool('l')
	lsa    = lsOpts.Bool('a')
	lsh    = lsOpts.Bool('h')
	lsR    = lsOpts.Bool('R')

	rmOpts = getopt.New()
	rmr    = rmOpts.Bool('r')
	rmf    = rmOpts.Bool('f')
	rmskipTrash = rmOpts.BoolLong("skipTrash", 0, "option bypasses trash, if enabled, and immediately deletes FILE")
	rmforceTrash = rmOpts.BoolLong("forceTrash", 0, "skip checking whether the trash is enabled on server side")

	mvOpts = getopt.New()
	mvn    = mvOpts.Bool('n')
	mvT    = mvOpts.Bool('T')

	mkdirOpts = getopt.New()
	mkdirp    = mkdirOpts.Bool('p')

	testOpts = getopt.New()
	teste    = testOpts.Bool('e')
	testf    = testOpts.Bool('f')
	testd    = testOpts.Bool('d')
	testz    = testOpts.Bool('z')
	tests    = testOpts.Bool('s')

	touchOpts = getopt.New()
	touchc    = touchOpts.Bool('c')

	chmodOpts = getopt.New()
	chmodR    = chmodOpts.Bool('R')

	chownOpts = getopt.New()
	chownR    = chownOpts.Bool('R')

	headTailOpts = getopt.New()
	headtailn    = headTailOpts.Int64('n', -1)
	headtailc    = headTailOpts.Int64('c', -1)

	duOpts = getopt.New()
	dus    = duOpts.Bool('s')
	duh    = duOpts.Bool('h')

	getmergeOpts = getopt.New()
	getmergen    = getmergeOpts.Bool('n')

	dfOpts = getopt.New()
	dfh    = dfOpts.Bool('h')

	cachedClients map[string]*hdfs.Client = make(map[string]*hdfs.Client)
	status                                = 0
)

func init() {
	lsOpts.SetUsage(printHelp)
	rmOpts.SetUsage(printHelp)
	mvOpts.SetUsage(printHelp)
	touchOpts.SetUsage(printHelp)
	chmodOpts.SetUsage(printHelp)
	chownOpts.SetUsage(printHelp)
	headTailOpts.SetUsage(printHelp)
	duOpts.SetUsage(printHelp)
	getmergeOpts.SetUsage(printHelp)
	dfOpts.SetUsage(printHelp)
	testOpts.SetUsage(printHelp)
}

func main() {
	if len(os.Args) < 2 {
		fatalWithUsage()
	}

	command := os.Args[1]
	argv := os.Args[1:]
	switch command {
	case "-v", "--version":
		fatal("gohdfs version", version)
	case "ls":
		lsOpts.Parse(argv)
		ls(lsOpts.Args(), *lsl, *lsa, *lsh, *lsR)
	case "rm":
		rmOpts.Parse(argv)
		rm(rmOpts.Args(), *rmr, *rmf, *rmskipTrash, *rmforceTrash)
	case "mv":
		mvOpts.Parse(argv)
		mv(mvOpts.Args(), !*mvn, *mvT)
	case "mkdir":
		mkdirOpts.Parse(argv)
		mkdir(mkdirOpts.Args(), *mkdirp)
	case "touch":
		touchOpts.Parse(argv)
		touch(touchOpts.Args(), *touchc)
	case "chown":
		chownOpts.Parse(argv)
		chown(chownOpts.Args(), *chownR)
	case "chmod":
		chmodOpts.Parse(argv)
		chmod(chmodOpts.Args(), *chmodR)
	case "cat":
		cat(argv[1:])
	case "head", "tail":
		headTailOpts.Parse(argv)
		printSection(headTailOpts.Args(), *headtailn, *headtailc, (command == "tail"))
	case "du":
		duOpts.Parse(argv)
		du(duOpts.Args(), *dus, *duh)
	case "checksum":
		checksum(argv[1:])
	case "get":
		get(argv[1:])
	case "getmerge":
		getmergeOpts.Parse(argv)
		getmerge(getmergeOpts.Args(), *getmergen)
	case "put":
		put(argv[1:])
	case "df":
		dfOpts.Parse(argv)
		df(*dfh)
	case "test":
		testOpts.Parse(argv)
		test(testOpts.Args(), *teste, *testf, *testd, *testz, *tests)
	case "setrep":
		setrep(argv[1:])
	case "truncate":
		truncate(argv[1:])
	// it's a seeeeecret command
	case "complete":
		complete(argv)
	case "help", "-h", "-help", "--help":
		printHelp()
	case "print-minimal-config":
		printMinimalConfig()
	default:
		fatalWithUsage("Unknown command:", command)
	}

	os.Exit(status)
}

func printHelp() {
	fmt.Fprintln(os.Stderr, usage)
	os.Exit(0)
}

func fatal(msg ...interface{}) {
	fmt.Fprintln(os.Stderr, msg...)
	os.Exit(1)
}

func fatalWithUsage(msg ...interface{}) {
	if len(msg) > 0 {
		fmt.Fprintln(os.Stderr, append(msg, "\n"+usage)...)
	} else {
		fmt.Fprintln(os.Stderr, usage)
	}

	os.Exit(2)
}

var (
	conf hadoopconf.HadoopConf
	confOnce sync.Once
	confErr error

	krbClient *krb.Client
	krbClientOnce sync.Once
	krbClientErr error

	username string
	userOnce sync.Once
	userErr error
)
func getConf() (*hadoopconf.HadoopConf, error) {
	confOnce.Do(func() {
		conf, confErr = hadoopconf.LoadFromEnvironment()
	})
	return &conf, confErr
}
func getKrbClientOnce() (*krb.Client, error) {
	krbClientOnce.Do(func() {
		krbClient, krbClientErr = getKerberosClient()
	})
	return krbClient, krbClientErr
}
func getUser() (*string, error) {
	userOnce.Do(func() {
		conf, err := getConf()
		if err != nil {
			username, userErr = "", err
		} else if strings.ToLower((*conf)["hadoop.security.authentication"]) == "kerberos" {
			// kerberos
			krb, err := getKrbClientOnce()
			if err != nil {
				username, userErr = "", err
			} else {
				username, userErr = krb.Credentials.UserName(), nil
			}
		} else {
			// non-kerberos
			username = os.Getenv("HADOOP_USER_NAME")
			if username != "" {
				userErr = nil
			} else {
				u, err := user.Current()
				if err == nil {
					username, userErr = u.Username, err
				} else {
					username, userErr = "", err
				}
			}
		}
	})
	return &username, userErr
}

func getClient(namenode string) (*hdfs.Client, error) {
	if cachedClients[namenode] != nil {
		return cachedClients[namenode], nil
	}

	if namenode == "" {
		namenode = os.Getenv("HADOOP_NAMENODE")
	}

	conf, err := getConf()
	if err != nil {
		return nil, fmt.Errorf("Problem loading configuration: %s", err)
	}

	options := hdfs.ClientOptionsFromConf(*conf)
	if namenode != "" {
		options.NSID = namenode
		switch conf.CheckTypeOfNameAddressString(namenode) {
		case hadoopconf.TNAS_SimpleAddress:
			options.Addresses = strings.Split(namenode, ",")
			if len(options.Addresses) > 0 {
				options.NSID = options.Addresses[0]
			}
		case hadoopconf.TNAS_SimpleNameServiceID:
			options.Addresses = conf.AddressesByNameServiceID(namenode)
		case hadoopconf.TNAS_ViewfsNameServiceID:
			return nil, errors.New("Cannot create hdfs Client from nsid " + namenode)
		}
	}

	if options.Addresses == nil {
		return nil, errors.New("Couldn't find a namenode to connect to. You should specify hdfs://<namenode>:<port> in your paths. Alternatively, set HADOOP_NAMENODE or HADOOP_CONF_DIR in your environment.")
	}

	if options.KerberosClient != nil {
		options.KerberosClient, err = getKrbClientOnce()
		if err != nil {
			return nil, fmt.Errorf("Problem with kerberos authentication: %s", err)
		}
	} else {
		u, err := getUser()
		if err != nil {
			return nil, fmt.Errorf("Couldn't determine user: %s", err)
		}
		options.User = *u
	}

	// Set some basic defaults.
	dialFunc := (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 5 * time.Second,
		DualStack: true,
	}).DialContext

	options.NamenodeDialFunc = dialFunc
	options.DatanodeDialFunc = dialFunc

	c, err := hdfs.NewClient(options)
	if err != nil {
		return nil, fmt.Errorf("Couldn't connect to namenode: %s", err)
	}

	cachedClients[namenode] = c
	return c, nil
}

func printMinimalConfig() {
	conf, err := getConf()
	if err != nil {
		fatal("Cannot load hadoop conf", err)
	}

	known_keys := map[string]struct{}{
		"dfs.client.use.datanode.hostname": {},
		"dfs.data.transfer.protection": {},
		"dfs.encrypt.data.transfer": {},
		"dfs.namenode.kerberos.principal": {},
		"dfs.nameservices": {},
		"fs.default.name": {},
		"fs.defaultFS": {},
	}

	miniConf := make(hadoopconf.HadoopConf)
	for k, v := range *conf {
		if _, ok := known_keys[k]; ok {
			miniConf[k] = v
		} else if strings.HasPrefix(k, "dfs.namenode.rpc-address.") {
			miniConf[k] = v
		} else if strings.HasPrefix(k, "dfs.ha.namenodes.") {
			miniConf[k] = v
		} else if strings.HasPrefix(k, "fs.viewfs.mounttable.") {
			miniConf[k] = v
		}
	}

	miniConf.MarshalXMLFile(os.Stdout)
}
