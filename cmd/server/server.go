// For now we'll do the tidwall/btree in memory implementation of single
// node server for persistant kv store and a client for interacting
// using tidwall/btree instead of google/btree, examples are simpler to understand
// TODO: replace of tidwall/btree with our own implementation

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	pb "github.com/iips-oss/distributed-kv/protobuf"
	"github.com/tidwall/btree"
	"google.golang.org/grpc"
)

// INFO: https://en.wikipedia.org/wiki/Copy-on-write
// tidwall/btree library has a few useful funcitons,
// like bulk loading keys with Load() and Copy() copy-on-write

// reason to use btree instead of Hashmap, is that btree preserve the
// lexical order of keys which is efficent for bulk-match key retreval.

var (
	port = flag.Int("port", 50051, "sever port")
)

// INFO: moved store global -> struct member, both works fine,
// its just that examples recommends to keep data structs as members
type server struct {
	pb.UnimplementedKvstoreServer
	store btree.Map[string, string]
}

// these are methods to server struct which is how it implements the KvstoreServer interface
// https://gobyexample.com/interfaces
// TODO: add RWMutex locks for sync? also related read
// https://oneuptime.com/blog/post/2026-01-23-go-mutex/view
func (s *server) KvGet(_ context.Context, in *pb.OpKeyReq) (*pb.OpGetRes, error) {
	key := in.GetKey()
	log.Printf("log: GET %v", key)
	value, ok := s.store.Get(key)
	if value == "" {
		value = "(nil)"
	}
	log.Printf("store: GET %v = %v : %v", key, value, ok)
	return &pb.OpGetRes{Value: value}, nil
}

func (s *server) KvSet(_ context.Context, in *pb.SetReq) (*pb.OpRes, error) {
	key := in.GetKey()
	value := in.GetValue()
	log.Printf("log: SET %v %v", key, value)
	s.store.Set(key, value)
	set_value, _ := s.store.Get(key)
	log.Printf("store: SET %v = %v", key, set_value)
	return &pb.OpRes{}, nil
}

func (s *server) KvDel(_ context.Context, in *pb.OpKeyReq) (*pb.OpRes, error) {
	key := in.GetKey()
	log.Printf("log: DEL %v", key)
	s.store.Delete(key)
	return &pb.OpRes{}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKvstoreServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// Iterate over btree and print it in order
func printMap(kv btree.Map[string, string]) {
	kv.Scan(func(key string, value string) bool {
		fmt.Printf("%s %s\n", key, value)
		return true
	})
}
