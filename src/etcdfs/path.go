package etcdfs

import(
  "log"
  "strings"

  "github.com/coreos/etcd/client"

  "github.com/hanwen/go-fuse/fuse"
  "github.com/hanwen/go-fuse/fuse/nodefs"
  "github.com/hanwen/go-fuse/fuse/pathfs"

	"golang.org/x/net/context"
)

type EtcdFs struct {
  pathfs.FileSystem
	client.Client
	client.KeysAPI
}

func (me *EtcdFs) Unlink(name string, ctx *fuse.Context) (code fuse.Status) {
  if name == "" {
    return fuse.OK
  }
	_, err := me.KeysAPI.Delete(context.Background(), name, nil)

  if err != nil {
    log.Println(err)
    return fuse.ENOENT
  }

  return fuse.OK
}

func (me *EtcdFs) Rmdir(name string, ctx *fuse.Context) (code fuse.Status) {
  if name == "" {
    return fuse.OK
  }

	_, err := me.KeysAPI.Delete(context.Background(), name, &client.DeleteOptions{ Dir: true })

  if err != nil {
    log.Println(err)
    return fuse.ENOENT
  }

  return fuse.OK
}

func (me *EtcdFs) Create(name string, flags uint32, mode uint32, ctx *fuse.Context) (file nodefs.File, code fuse.Status) {
	_, err := me.KeysAPI.Set(context.Background(), name, "", nil)

  if err != nil {
    log.Println("Create Error:", err)
    return nil, fuse.ENOENT
  }

  return NewEtcdFile(me.KeysAPI, name), fuse.OK
}

func (me *EtcdFs) Mkdir(name string, mode uint32, ctx *fuse.Context) fuse.Status {
  if name == "" {
    return fuse.OK
  }

	_, err := me.KeysAPI.Set(context.Background(), name, "", &client.SetOptions{ Dir: true })

  if err != nil {
    log.Println(err)
    return fuse.ENOENT
  }

  return fuse.OK
}

func (me *EtcdFs) GetAttr(name string, ctx *fuse.Context) (*fuse.Attr, fuse.Status) {
  if name == "" {
    return &fuse.Attr{
      Mode: fuse.S_IFDIR | 0666,
    }, fuse.OK
  }

  res, err := me.KeysAPI.Get(context.Background(), name, nil)

  if err != nil {
    return nil, fuse.ENOENT
  }

  var attr fuse.Attr

  if res.Node.Dir {
    attr = fuse.Attr{
      Mode: fuse.S_IFDIR | 0666,
    }
  } else {
    attr = fuse.Attr{
      Mode: fuse.S_IFREG | 0666, Size: uint64(len(res.Node.Value)),
    }
  }

  return &attr, fuse.OK
}

func (me *EtcdFs) OpenDir(name string, ctx *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
  res, err := me.KeysAPI.Get(context.Background(), name, nil)

  if err != nil {
    log.Println("OpenDir Error:", err)
    return nil, fuse.ENOENT
  }

  entries := []fuse.DirEntry{}

  for _, e := range(res.Node.Nodes) {
    chunks := strings.Split(e.Key, "/")
    file := chunks[len(chunks)-1]
    if e.Dir {
      entries = append(entries, fuse.DirEntry{Name: file, Mode: fuse.S_IFDIR})
    } else {
      entries = append(entries, fuse.DirEntry{Name: file, Mode: fuse.S_IFREG})
    }
  }

  return entries, fuse.OK
}

func (me *EtcdFs) Open(name string, flags uint32, ctx *fuse.Context) (file nodefs.File, code fuse.Status) {
	_, err := me.KeysAPI.Get(context.Background(), name, nil)

  if err != nil {
    log.Println("Open Error:", err)
    return nil, fuse.ENOENT
  }

  return NewEtcdFile(me.KeysAPI, name), fuse.OK
}

