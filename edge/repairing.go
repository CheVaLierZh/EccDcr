package main

import (
	"EccDcr/core"
	"EccDcr/pb"
	"context"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type RepairService struct {
	preGraph map[uint64]*core.Cluster64
	graph map[uint64]*core.Cluster64
	sender *core.Cluster64
	vc *core.Cluster64
	preVC *core.Cluster64
	preVCMu sync.Mutex
	waitCandsBalance int32
	vcMu sync.Mutex
	mu sync.Mutex
	repairs *core.Cluster
	waitTurn int32
}

func (rs *RepairService) PutToVC(val *core.Cluster64) {
	rs.vcMu.Lock()
	defer rs.vcMu.Unlock()
	rs.vc.Or(val)
}

func (w *Worker) RepairInit(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	w.MakeRepairService()
	//log.Println("finish repair init")
	return &emptypb.Empty{}, nil
}

func (w *Worker) MakeRepairService() {
	res := &RepairService{vc: core.MakeCluster64()}
	res.preGraph = w.cds.GetFinalResult()
	tmp := make(map[uint64]*core.Cluster64)
	for k, v := range res.preGraph {
		for iter := v.Iterator(); iter.HasNext(); {
			r := iter.Next()
			if _, ok := tmp[r]; !ok {
				tmp[r] = core.MakeCluster64()
			}
			tmp[r].Add(k)
		}
	}

	cnt := 0
	for _, v := range res.preGraph {
		cnt += int(v.Size())
	}

	//log.Println("preGraph pre size:", cnt)

	for k, v := range tmp {
		if _, ok := res.preGraph[k]; !ok {
			res.preGraph[k] = core.MakeCluster64()
		}
		res.preGraph[k].Or(v)
	}

	cnt = 0
	for _, v := range res.preGraph {
		cnt += int(v.Size())
	}

	//log.Println("preGraph size:", cnt)

	res.graph = make(map[uint64]*core.Cluster64)
	w.rs = res
	w.cds = nil
}


func (w *Worker) RepairBalance(ctx context.Context, args *emptypb.Empty) (*pb.CBBalanceReply, error) {

	atomic.StoreInt32(&w.rs.waitCandsBalance, 0)
	go w.repairBalance()
	return &pb.CBBalanceReply{WorkLoad: 0}, nil
}

func (w *Worker) repairBalance() {
	streams := make([]pb.Worker_RepairPushCandidatesClient, 0, len(w.peers))
	for i, peer := range w.peers {
		if i != w.id {
			s, _ := peer.RepairPushCandidates(context.Background())
			streams = append(streams, s)
		} else {
			streams = append(streams, nil)
		}
	}
	for k, v := range w.rs.preGraph {
		belong := int(k % uint64(len(w.peers)))
		if belong != w.id {
			streams[belong].Send(&pb.PushCandidatesArgs{Key: k, Val: v.ToBytes()})
		} else {
			w.rs.mu.Lock()
			if _, ok := w.rs.graph[k]; !ok {
				w.rs.graph[k] = v
			} else {
				w.rs.graph[k].Or(v)
			}
			w.rs.mu.Unlock()
		}
	}

	for i, stream := range streams {
		if i != w.id {
			stream.CloseAndRecv()
		}
	}
	w.rs.preGraph = nil
	atomic.AddInt32(&w.rs.waitCandsBalance, 1)

	for atomic.LoadInt32(&w.rs.waitCandsBalance) != int32(len(w.peers)) {
		//log.Println("wait cands", atomic.LoadInt32(&w.rs.waitCandsBalance))
		time.Sleep(time.Duration(200) * time.Millisecond)
	}

	//log.Println("graph size:", len(w.rs.graph))
	w.coordinator.RepairBalanceComplete(context.Background(), &emptypb.Empty{})
}

func (w *Worker) RepairPushCandidates(stream pb.Worker_RepairPushCandidatesServer) error {
	for {
		args, err := stream.Recv()
		if err == io.EOF {
			atomic.AddInt32(&w.rs.waitCandsBalance, 1)
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}
		adj := core.MakeCluster64()
		adj.FromBytes(args.Val)
		w.rs.mu.Lock()
		if _, ok := w.rs.graph[args.Key]; !ok {
			w.rs.graph[args.Key] = adj
		} else {
			w.rs.graph[args.Key].Or(adj)
		}
		w.rs.mu.Unlock()
	}
}

func (rs *RepairService) vertexCover(sender *core.Cluster64) {
	if sender.Size() != 0 {

		sd := core.MakeCluster64()
		rv := core.MakeCluster64()
		for k, v := range rs.graph {
			if !rs.sender.Contains(k) {
				u := v.DeepCopy()
				u.And(sender)
				r := uint64(0)
				for iter := u.Iterator(); iter.HasNext(); {
					x := iter.Next()
					if !sd.Contains(x) {
						r = x
						break
					}
				}
				if r == 0 {
					continue
				}
				rs.preVCMu.Lock()
				if !rs.preVC.Contains(k) && !rs.preVC.Contains(r) {
					sd.Add(r)
					rv.Add(k)
					rs.preVC.Add(k)
					rs.preVC.Add(r)
				}
				rs.preVCMu.Unlock()
			}
		}

		rs.PutToVC(rv)
		rs.PutToVC(sd)
	}

	atomic.AddInt32(&rs.waitTurn, 1)
}

func (rs *RepairService) removeIncidentEdges(vs *core.Cluster64) {
	if vs.Size() == 0 {
		return
	}
	for k, v := range rs.graph {
		if vs.Contains(k) {
			delete(rs.graph, k)
		} else {
			v.AndNot(vs)
			if v.Size() == 0 {
				delete(rs.graph, k)
			}
		}
	}
}

func (w *Worker) turn() {
	log.Println("new turn ....")

	var wg sync.WaitGroup
	for i := 0; i < len(w.peers); i++ {
		wg.Add(1)
		if w.id == i {
			go func() {
				w.rs.vertexCover(w.rs.sender)
				wg.Done()
			} ()
		} else {
			go func(j int) {
				w.peers[j].RepairVertexCover(context.Background(), &pb.Cluster64Message{Cluster: w.rs.sender.ToBytes()})
				wg.Done()
			} (i)
		}
	}
	wg.Wait()
	for atomic.LoadInt32(&w.rs.waitTurn) != int32(len(w.peers)) {
		//log.Println("wait turn:", atomic.LoadInt32(&w.rs.waitTurn))
		time.Sleep(time.Duration(200) * time.Millisecond)
	}

	restEdges := int32(0)
	for _, v := range w.rs.graph {
		restEdges += int32(v.Size())
	}

	//log.Println("rest edge:", restEdges)

	w.coordinator.RepairTurnComplete(context.Background(), &pb.RepairTurnArgs{RestEdges: restEdges, Cluster: w.rs.vc.ToBytes()})
	w.rs.vc = nil
}

func (w *Worker) RepairVertexCover(ctx context.Context, args *pb.Cluster64Message) (*emptypb.Empty, error) {
	sender := core.MakeCluster64()
	sender.FromBytes(args.Cluster)
	for true {
		w.rs.vcMu.Lock()
		if w.rs.vc != nil {
			w.rs.vcMu.Unlock()
			break
		}
		w.rs.vcMu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	w.rs.vertexCover(sender)
	return &emptypb.Empty{}, nil
}

func (w *Worker) RepairTurn(ctx context.Context, args *pb.Cluster64Message) (*emptypb.Empty, error) {
	vc := core.MakeCluster64()
	vc.FromBytes(args.Cluster)
	w.rs.vcMu.Lock()
	w.rs.waitTurn = 0
	w.rs.vc = vc
	w.rs.preVC = core.MakeCluster64()

	w.rs.removeIncidentEdges(w.rs.vc)

	w.rs.sender = core.MakeCluster64()
	for k := range w.rs.graph {
		if rand.Float64() < 0.5 {
			w.rs.sender.Add(k)
		}
	}

	w.rs.vcMu.Unlock()

	go w.turn()
	return &emptypb.Empty{}, nil
}

func (w *Worker) RepairAggr(ctx context.Context, args *pb.RepairAggrArgs) (*emptypb.Empty, error) {
	w.rs.repairs = core.MakeCluster()
	w.rs.repairs.FromBytes(args.Vc)

	w.ss.localTable.DoRepair(w.rs.repairs)
	return &emptypb.Empty{}, nil
}

