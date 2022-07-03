package main

import (
	"EccDcr/core"
	"EccDcr/pb"
	"context"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type RepairManager struct {
	waitBalancing int32
	waitTurn int32
	mu sync.Mutex
	turns int
	restEdges int32
	vc *core.Cluster64
	vcMu sync.Mutex
}

func (c *Coordinator)MakeRepairManager() *RepairManager {
	res := &RepairManager{waitBalancing: 0, waitTurn: 0, turns: 0,restEdges: 1 << 31 - 1}
	res.vc = core.MakeCluster64()
	return res
}

func (c *Coordinator) repairBalancing() {
	c.rm.waitBalancing = 0

	for i := 0; i < len(c.followers); i++ {
		go func(j int) {
			c.followers[j].RepairBalance(context.Background(), &emptypb.Empty{})
		} (i)
	}
}

func (c *Coordinator) RepairBalanceComplete(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	atomic.AddInt32(&c.rm.waitBalancing, 1)
	return &emptypb.Empty{}, nil
}

func (c *Coordinator) RepairTurnComplete(ctx context.Context, args *pb.RepairTurnArgs) (*emptypb.Empty, error) {
	cls := core.MakeCluster64()
	cls.FromBytes(args.Cluster)
	c.rm.vcMu.Lock()
	defer c.rm.vcMu.Unlock()
	c.rm.vc.Or(cls)
	atomic.AddInt32(&c.rm.waitTurn, 1)
	atomic.AddInt32(&c.rm.restEdges, args.RestEdges)
	return &emptypb.Empty{}, nil
}

func (c *Coordinator) repairTurn() {
	c.rm.waitTurn = 0
	c.rm.restEdges = 0
	var wg sync.WaitGroup
	for i := 0; i < len(c.followers); i++ {
		wg.Add(1)
		go func(j int) {
			c.followers[j].RepairTurn(context.Background(), &pb.Cluster64Message{Cluster: c.rm.vc.ToBytes()})
			wg.Done()
		} (i)
	}
	wg.Wait()

	c.rm.vcMu.Lock()
	defer c.rm.vcMu.Unlock()
	c.rm.vc = core.MakeCluster64()
}

func (c *Coordinator) repairInit() {
	var wg sync.WaitGroup
	for i := 0; i < len(c.followers); i++ {
		wg.Add(1)
		go func(j int) {
			c.followers[j].RepairInit(context.Background(), &emptypb.Empty{})
			wg.Done()
		} (i)
	}
	wg.Wait()
}


func (c *Coordinator) Repair(doRepair bool) {
	c.rm = c.MakeRepairManager()
	c.rm.waitBalancing = 0
	log.Println("start repair balancing...")
	start := time.Now()

	c.repairInit()

	c.repairBalancing()
	for atomic.LoadInt32(&c.rm.waitBalancing) != int32(len(c.followers)) {
		time.Sleep(time.Duration(200) * time.Millisecond)
	}
	//t1 := time.Now()
	//log.Println("repair balance cost time:", t1.Sub(start).Seconds(), "s")

	log.Println("start repair...")
	for c.rm.restEdges != 0 && c.rm.turns < 64 {
		log.Println("repair turn: ", c.rm.turns)
		c.rm.restEdges = 0
		c.rm.waitTurn = 0
		c.repairTurn()
		for atomic.LoadInt32(&c.rm.waitTurn) != int32(len(c.followers)) {
			time.Sleep(time.Duration(200) * time.Millisecond)
		}
		c.rm.turns++
		log.Println("repair rest edges:", c.rm.restEdges)
	}

	result := c.rm.vc.Decompose()
	if doRepair {
		var wg sync.WaitGroup
		for i := 0; i < len(c.followers); i++ {
			wg.Add(1)
			go func(j int) {
				if _, ok := result[j]; ok {
					c.followers[j].RepairAggr(context.Background(), &pb.RepairAggrArgs{Vc: result[j].ToBytes()})
				} else {
					c.followers[j].RepairAggr(context.Background(), &pb.RepairAggrArgs{Vc: core.MakeCluster().ToBytes()})
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	}

	log.Println("repair total cost time:", time.Now().Sub(start).Seconds(), "s")

	log.Println("repair delete row num:", c.rm.vc.Size())
	for k, v := range result {
		log.Println("table of machine", k, "delete row num:", v.Size())
	}
}






