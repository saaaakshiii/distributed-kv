// centralized server connecting gRPC client

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pb "github.com/iips-oss/distributed-kv/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
)

func main() {
	flag.Parse()
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("didn't connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKvstoreClient(conn)
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "exit" {
			fmt.Printf("QUITTING\n")
			return
		}
		cmd := strings.Split(line, " ")
		if len(cmd) > 3 || len(cmd) < 2 {
			fmt.Printf("Error: invalid command syntax\n")
			continue
		}
		method := cmd[0]
		key := cmd[1]

		switch method {
		case "GET":
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			r, err := c.KvGet(ctx, &pb.OpKeyReq{Key: key})
			cancel()
			if err != nil {
				log.Fatal(err)
			}
			value := r.GetValue()
			if value != "" {
				fmt.Printf("%s\n", r.GetValue())
			}
		case "SET":
			if len(cmd) != 3 {
				fmt.Printf("Error: two arguments required\n")
			}
			value := cmd[2]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := c.KvSet(ctx, &pb.SetReq{Key: key, Value: value})
			cancel()
			if err != nil {
				log.Fatal(err)
			}
		case "DEL":
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := c.KvDel(ctx, &pb.OpKeyReq{Key: key})
			cancel()
			if err != nil {
				log.Fatal(err)
			}
		case "exit": // maybe handle ctrl+c/d singles too
			fmt.Printf("QUITING\n")
			return
		}
	}
}
