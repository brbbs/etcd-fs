package main

import (
  . "etcdfs"
  "flag"
  "log"
	"os"
	"strings"

  "github.com/coreos/etcd/client"

  "github.com/hanwen/go-fuse/fuse/nodefs"
  "github.com/hanwen/go-fuse/fuse/pathfs"
)

func main() {
  flag.Parse()
  if len(flag.Args()) < 2 {
    log.Fatal("Usage:\n  etcd-fs MOUNTPOINT ETCDENDPOINTS")
  }
	c, err := client.New(client.Config {
		Endpoints: strings.Split(flag.Arg(1), ","),
		Username: os.ExpandEnv("$ETCD_USERNAME"),
		Password: os.ExpandEnv("$ETCD_PASSWORD"),
	})
  etcdFs := EtcdFs{
		FileSystem: pathfs.NewDefaultFileSystem(),
		Client: c,
		KeysAPI: client.NewKeysAPI(c),
	}
  nfs := pathfs.NewPathNodeFs(&etcdFs, nil)
  server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
  if err != nil {
    log.Fatalf("Mount fail: %v\n", err)
  }
  server.Serve()
}
