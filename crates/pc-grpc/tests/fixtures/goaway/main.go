package main

import (
	"context"
	"flag"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
)

type connectionLog struct {
	name string
}

func (h connectionLog) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (h connectionLog) HandleRPC(_ context.Context, _ stats.RPCStats)                   {}
func (h connectionLog) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (h connectionLog) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		log.Printf("%s connection_open", h.name)
	case *stats.ConnEnd:
		log.Printf("%s connection_close", h.name)
	}
}

func main() {
	addrOne := flag.String("addr-one", "127.0.0.1:19106", "first listener")
	addrTwo := flag.String("addr-two", "127.0.0.1:19107", "second listener")
	idle := flag.Duration("idle", 15*time.Second, "MaxConnectionIdle")
	flag.Parse()

	serve(*addrOne, *idle)
	serve(*addrTwo, *idle)
	select {}
}

func serve(address string, idle time.Duration) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer(
		grpc.StatsHandler(connectionLog{name: address}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: idle, MaxConnectionAge: 30 * time.Minute,
			MaxConnectionAgeGrace: 30 * time.Second, Time: 30 * time.Second,
			Timeout: 5 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime: 5 * time.Second, PermitWithoutStream: true,
		}),
		grpc.ConnectionTimeout(10*time.Second),
	)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	log.Printf("%s listening max_connection_idle=%s", address, idle)
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("%s serve ended: %v", address, err)
		}
	}()
}
