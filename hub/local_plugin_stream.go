package hub

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
)

const localPluginStreamBuffer = 1024

type localPluginStream struct {
	ctx  context.Context
	rows chan *proto.ExecuteResponse
}

func newLocalStream(ctx context.Context) *localPluginStream {
	return &localPluginStream{
		ctx:  ctx,
		rows: make(chan *proto.ExecuteResponse, localPluginStreamBuffer),
	}
}
func (s *localPluginStream) Send(r *proto.ExecuteResponse) error {
	log.Printf("[WARN] localPluginStream Send")
	s.rows <- r
	return nil
}

func (s *localPluginStream) Recv() (*proto.ExecuteResponse, error) {
	resp := <-s.rows
	log.Printf("[WARN] localPluginStream Recv %v", resp)
	return resp, nil
}

func (s *localPluginStream) Context() context.Context {
	return s.ctx
}
