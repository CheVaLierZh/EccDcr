package main

import (
	"EccDcr/core"
	"EccDcr/pb"
	"context"
	"google.golang.org/protobuf/types/known/emptypb"
	"math/rand"
)

type EvaluateService struct {
	sampleRows []uint32
}

func (w *Worker) MakeEvaluateService() *EvaluateService {
	return &EvaluateService{}
}

func (w *Worker) TableSize(ctx context.Context, _ *emptypb.Empty) (*pb.SizeMessage, error) {
	w.es = w.MakeEvaluateService()
	return &pb.SizeMessage{Size: int32(w.ss.localTable.Size())}, nil
}

func (w *Worker) Sampling(ctx context.Context, args *pb.SizeMessage) (*emptypb.Empty, error) {
	size := int(args.Size)
	w.es.sampleRows = make([]uint32, size)
	aliveRows := core.MakeCluster()
	for i := 0; i < size; i++ {
		r := (rand.Uint32() % w.ss.localTable.Size()) + 1
		w.es.sampleRows[i] = r
		aliveRows.Add(r)
	}
	w.ss.localTable.SetAliveRows(aliveRows)
	return &emptypb.Empty{}, nil
}
