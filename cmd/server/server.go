// For now we'll do the tidwall/btree in memory implementation of single
// node server for persistant kv store and a client for interacting
// using tidwall/btree instead of google/btree, examples are simpler to understand
// TODO: replace of tidwall/btree with our own implementation

package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	pb "github.com/iips-oss/distributed-kv/protobuf"
	"google.golang.org/grpc"
	"modernc.org/b/v2"
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
	store *b.Tree[string, string]
}

// Creates a new B+ tree, intializing the store
func NewServer() *server {
	return &server{
		store: b.TreeNew[string, string](cmp.Compare),
	}
}

// these are methods to server struct which is how it implements the KvstoreServer interface
// https://gobyexample.com/interfaces
// TODO: add RWMutex locks for sync? also related read
// https://oneuptime.com/blog/post/2026-01-23-go-mutex/view

// > GET ispark
// ispark=abc
// ispark_port_num=8000
// ispark_mongodb_uri=dbkhd25361
// ispark_api_key=dhdhiuwhq
func (s *server) KvGet(_ context.Context, in *pb.OpKeyReq) (*pb.OpGetRes, error) {
	key_ip := in.GetKey() // from client
	log.Printf("log: GET %v", key_ip)
	result := []*pb.KeyValuePair{}
	e, err := s.store.SeekFirst() // first element
	if err != nil {
		return nil, err
	}

	for {
		key, _, err := e.Next() // will return the current item and move to the next
		if err != nil {         // if end of file reached, break
			break
		}
		i := 0 // 1st character

		for i < len(key_ip) && i < len(key) {
			if key_ip[i] == key[i] {
				i++
			} else if key_ip[i] != key[i] {
				break
			}
		}
		if i == len(key_ip) { // all characters matched
			value, ok := s.store.Get(key)
			if !ok {
				return nil, err
			}
			result = append(result, &pb.KeyValuePair{Key: key, Value: value})
		}

	}
	if len(result) == 0 {
		result = nil
	}
	return &pb.OpGetRes{KeyValuePairs: result}, nil
}

func (s *server) KvSet(_ context.Context, in *pb.SetReq) (*pb.OpRes, error) {
	key := in.GetKey()     // from client
	value := in.GetValue() // from client
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
	// srv := NewServer()
	srv := &server{
		store: b.TreeNew[string, string](cmp.Compare),
	}
	pb.RegisterKvstoreServer(s, srv)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func printMap(kv *b.Tree[string, string]) {
	e, err := kv.SeekFirst()
	if err != nil {
		return
	}
	for {
		key, val, err := e.Next() // will return the current item and move to the next
		if err == io.EOF {        // if end of file reached, break
			break
		}
		if err != nil {
			return
		}

		fmt.Printf("%s %s\n", key, val)
	}
}
