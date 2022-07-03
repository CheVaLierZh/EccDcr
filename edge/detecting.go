package main

import (
	"EccDcr"
	"EccDcr/cc"
	"EccDcr/core"
	"EccDcr/myutil"
	"EccDcr/pb"
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ConflictDetectService struct {
	currentPlans []EccDcr.LogicPlan
	currPlansInput map[uint64]*core.Cluster64
	balanceAdd map[uint64]*core.Cluster64
	addMu sync.Mutex
	balanceDelete map[uint64]bool
	deleteMu sync.Mutex
	parentPlanId int
	interResults *core.PairClusterPersist
	dcResults *core.PairClusterPersist
	tdcResults *core.PairClusterPersist
	waitCandsBalance int32
	waitClusterIndex int32
	running int32

	sinceInferConflict map[int64][][2]uint64
	sinceInferTimeKeys []int64
}

func (cds *ConflictDetectService) InputDeepCopy() map[uint64]*core.Cluster64 {
	b := make(map[uint64]*core.Cluster64)
	for k, v := range cds.currPlansInput {
		b[k] = v.DeepCopy()
	}

	return b
}

func (cds *ConflictDetectService) DeleteFromInput(x uint64) {
	cds.deleteMu.Lock()
	defer cds.deleteMu.Unlock()
	cds.balanceDelete[x] = true
}

func (cds *ConflictDetectService) PutToInput(x uint64, c *core.Cluster64) {
	cds.addMu.Lock()
	defer cds.addMu.Unlock()
	cds.balanceAdd[x] = c
}

func (cds *ConflictDetectService) UpdateInput() {
	for k := range cds.balanceDelete {
		delete(cds.currPlansInput, k)
	}
	for k, v := range cds.balanceAdd {
		cds.currPlansInput[k] = v
	}
	cds.balanceDelete = nil
	cds.balanceAdd = nil
}

func (w *Worker)MakeConflictDetectService() {
	res := &ConflictDetectService{}
	res.interResults = core.MakePairClusterPersist("interResults")
	res.dcResults = core.MakePairClusterPersist("dcResults")
	res.tdcResults = core.MakePairClusterPersist("tdcResults")
	res.currPlansInput = make(map[uint64]*core.Cluster64)
	for iter := w.ss.localTable.GetAliveRows().Iterator(); iter.HasNext(); {
		res.currPlansInput[core.Encode(w.id, iter.Next())] = core.MakeCluster64Union()
	}

	w.cds = res
}

func (cds *ConflictDetectService) SetCurrentInput(parentPlanId int) {
	if parentPlanId != 0 {
		cds.currPlansInput = cds.interResults.Get(parentPlanId)
	}
	cds.balanceAdd = make(map[uint64]*core.Cluster64)
	cds.balanceDelete = make(map[uint64]bool)
	cds.parentPlanId = parentPlanId
	cds.waitCandsBalance = 0
}

func (cds *ConflictDetectService) SetCurrentPlans(plans []EccDcr.LogicPlan) {
	cds.currentPlans = plans
}

func (cds *ConflictDetectService) GetFinalResult() map[uint64]*core.Cluster64 {
	var res map[uint64]*core.Cluster64
	for _, key := range cds.tdcResults.GetAllKeys() {
		val := cds.tdcResults.Get(key)
		if res == nil {
			res = val
		} else {
			for k, v := range val {
				if _, ok := res[k]; !ok {
					res[k] = v
				} else {
					res[k].Or(v)
				}
			}
		}
	}

	for k, v := range res {
		if v.Size() == 0 {
			delete(res, k)
		}
	}

	return res
}

func (w *Worker) CBBalance(ctx context.Context, args *pb.BalanceArgs) (*pb.CBBalanceReply, error) {
	reply := &pb.CBBalanceReply{}

	if args.ParentPlanId == 0 {
		w.MakeConflictDetectService()
		atomic.StoreInt32(&w.cds.running, 1)
	}
	w.cds.SetCurrentInput(int(args.ParentPlanId))

	if args.ParentPlanId != 0 {
		total := uint64(0)
		for _, v := range w.cds.currPlansInput {
			total += v.Size()
		}
	}


	reply.WorkLoad = int32(len(w.cds.currPlansInput))
	//log.Println("col based balance origin workload", reply.WorkLoad)
	go w.balancing()
	return reply, nil
}

func (w *Worker) RBBalance(ctx context.Context, args *pb.BalanceArgs) (*pb.RBBalanceReply, error) {
	reply := &pb.RBBalanceReply{Distrib: make(map[uint64]int32)}

	if args.ParentPlanId == 0 {
		panic("row based balance can not be exec in the first run")
	}

	w.cds.SetCurrentInput(int(args.ParentPlanId))
	for k, v := range w.cds.currPlansInput {
		reply.Distrib[k] = int32(v.Size())
	}
	go w.balancing()
	return reply, nil
}

func (w *Worker) PushCBLP(ctx context.Context, args *pb.PushLPArgs) (*emptypb.Empty, error) {
	plans := make([]EccDcr.LogicPlan, len(args.Plans))
	for i := 0; i < len(args.Plans); i++ {
		plans[i].FromBytes(args.Plans[i])
	}
	w.cds.SetCurrentPlans(plans)
	go w.cbExecuting()
	return &emptypb.Empty{}, nil
}

func (w *Worker) PushRBLP(ctx context.Context, args *pb.PushLPArgs) (*emptypb.Empty, error) {
	plans := make([]EccDcr.LogicPlan, len(args.Plans))
	for i := 0; i < len(args.Plans); i++ {
		plans[i].FromBytes(args.Plans[i])
	}
	w.cds.SetCurrentPlans(plans)
	//go w.rbExecuting()
	return &emptypb.Empty{}, nil
}

func (w *Worker) PushCandidates(stream pb.Worker_PushCandidatesServer) error {
	defer atomic.AddInt32(&w.cds.waitCandsBalance, 1)
	for {
		args, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}
		adj := &core.Cluster64{}
		adj.FromBytes(args.Val)
		w.cds.PutToInput(args.Key, adj)
	}
}

func (w *Worker) GetConflictDetectResult() map[uint64]*core.Cluster64 {
	res := make(map[uint64]*core.Cluster64)
	for dcId := range w.cds.tdcResults.GetAllKeys() {
		for u, adj := range w.cds.tdcResults.Get(dcId) {
			if _, ok := res[u]; !ok {
				res[u] = core.MakeCluster64()
			}
			res[u].Or(adj)
		}
	}

	return res
}

func (w *Worker) ConflictDetectResult(ctx context.Context, _ *emptypb.Empty) (*pb.ConflictDetectResultReply, error) {
	reply := &pb.ConflictDetectResultReply{}
	reply.DcId = make([]int32, 0)
	reply.CCnt = make([]int32, 0)
	for dcId := range w.cds.tdcResults.GetAllKeys() {
		reply.DcId = append(reply.DcId, int32(dcId))
		total := int32(0)
		for _, adj := range w.cds.tdcResults.Get(dcId) {
			total += int32(adj.Size())
		}
		reply.CCnt = append(reply.CCnt, total)
	}

	total := w.GetConflictDetectResult()

	for _, v := range total {
		reply.Total += int32(v.Size())
	}

	return reply, nil
}

func (w *Worker) PullData(args *pb.PullDataArgs, stream pb.Worker_PullDataServer) error {
	table := args.Table
	c := core.MakeCluster()
	c.FromBytes(args.RowsId)
	if w.ss.localTable.GetTableName() == table {
		tIter := w.ss.localTable.Iterator()
		for iter := c.Iterator(); iter.HasNext(); {
			v := iter.Next()
			data :=tIter.NextFind(v)
			data = append(data, fmt.Sprint(v))
			stream.Send(&pb.PullDataReply{Data: data})
		}
	}

	return nil
}

func (w *Worker) cbBalanceWorkload(delivery []int32) {
	var wg sync.WaitGroup

	tmp := core.MakeCluster64()
	for k := range w.cds.currPlansInput {
		tmp.Add(k)
	}
	workload := tmp.Decompose()

	rest := 0
	for i, s := range delivery {
		if v, ok := workload[i]; !ok {
			rest += int(s)
		} else {
			rest += myutil.MaxInt(0, int(s)-v.Size())
		}
	}

	restSamples := make([]uint64, 0, rest)
	if _, ok := workload[w.id]; ok {
		for iter := workload[w.id].Iterator(); iter.HasNext(); {
			x := core.Encode(w.id, iter.Next())
			restSamples = append(restSamples, x)
			if len(restSamples) >= rest {
				break
			}
		}
	}

	restIdx := int32(-1)
	for i, s := range delivery {
		if i != w.id {
			wg.Add(1)
			go func(j int, d int) {
				stream, _ := w.peers[j].PushCandidates(context.Background())
				var iter core.IntIterable32
				k := 0
				size := 0
				if v, ok := workload[j]; ok {
					size = v.Size()
					iter = v.Iterator()
				}
				for ; k < myutil.MinInt(d, size); k++ {
					r := iter.Next()
					x := core.Encode(j, r)
					err := stream.Send(&pb.PushCandidatesArgs{Key: x, Val: w.cds.currPlansInput[x].ToBytes()})
					if err != nil {
						log.Println(err.Error())
					}
					w.cds.DeleteFromInput(x)
				}
				for ; k < d; k++ {
					idx := atomic.AddInt32(&restIdx, 1)
					x := restSamples[idx]
					stream.Send(&pb.PushCandidatesArgs{Key: x, Val: w.cds.currPlansInput[x].ToBytes()})
					w.cds.DeleteFromInput(x)
				}
				stream.CloseAndRecv()
				wg.Done()
			} (i, int(s))
		}
	}
	wg.Wait()
	atomic.AddInt32(&w.cds.waitCandsBalance, 1)  // self candidates balance complete
}

func (w *Worker) CBBalanceWorkload(ctx context.Context, args *pb.CBBalanceWorkloadArgs) (*emptypb.Empty, error) {

	go w.cbBalanceWorkload(args.Delivery)

	return &emptypb.Empty{}, nil
}

func (w *Worker) rbBalanceWorkload(allocate map[uint64]int32) {
	alloc := make(map[int]*core.Cluster64)
	for k, v := range allocate {
		if _, ok := alloc[int(v)]; !ok {
			alloc[int(v)] = core.MakeCluster64()
		}
		alloc[int(v)].Add(k)
	}

	var wg sync.WaitGroup
	for k, v := range alloc {
		wg.Add(1)
		go func(j int, u *core.Cluster64) {
			stream, _ := w.peers[j].PushCandidates(context.Background())

			for iter := u.Iterator(); iter.HasNext(); {
				r := iter.Next()
				stream.Send(&pb.PushCandidatesArgs{Key: r, Val: w.cds.currPlansInput[r].ToBytes()})
				w.cds.DeleteFromInput(r)
			}
			stream.CloseAndRecv()
			wg.Done()
		} (k, v)
	}

	wg.Wait()
	atomic.AddInt32(&w.cds.waitCandsBalance, 1)  // self candidates balance complete
}

func (w *Worker) RBBalanceWorkload(ctx context.Context, args *pb.RBBalanceWorkloadArgs) (*emptypb.Empty, error) {
	go w.rbBalanceWorkload(args.Allocate)
	return &emptypb.Empty{}, nil
}

func (w *Worker) balancing() {
	log.Println("start candidates balancing...")
	for atomic.LoadInt32(&w.cds.waitCandsBalance) != int32(len(w.peers)) {
		time.Sleep(time.Duration(500) * time.Millisecond)
	}
	w.cds.UpdateInput()
	//log.Println("input workload after balancing", len(w.cds.currPlansInput))
	input := make(map[int]*core.Cluster)

	for x := range w.cds.currPlansInput {
		m, y := core.Decode(x)
		if m != w.id {
			if _, ok := input[m]; !ok {
				input[m] = core.MakeCluster()
			}
			input[m].Add(y)
		}
	}

	log.Println("start data balancing...")
	w.ss.TmpTablesFlush()
	if len(w.peers) > 1 {
		var wg sync.WaitGroup
		for k, v := range input {
			wg.Add(1)
			go func(j int, u *core.Cluster) {
				args := &pb.PullDataArgs{Table: w.ss.localTable.GetTableName(), RowsId: u.ToBytes()}
				stream, _ := w.peers[j].PullData(context.Background(), args)
				w.ss.CreateTmpTable(j)
				buf := make([][]string, 0, BatchSize)
				cnt := 0
				for {
					reply, err := stream.Recv()
					if err == io.EOF {
						wg.Done()
						break
					}
					if err != nil {
						log.Fatal(err.Error())
					}
					buf = append(buf, reply.Data)
					if len(buf) > BatchSize {
						w.ss.tmpTables[j].Append(buf...)
						cnt += len(buf)
						buf = make([][]string, 0, BatchSize)
					}
				}
				if len(buf) != 0 {
					w.ss.tmpTables[j].Append(buf...)
					cnt += len(buf)
				}
				//log.Println("receive data row size", cnt, "for table", j)
			}(k, v)
		}
		wg.Wait()
	}

	w.cds.waitClusterIndex = 0
	w.coordinator.BalanceComplete(context.Background(), &pb.BalanceCompleteArgs{ParentPlanId: int32(w.cds.parentPlanId), MachineId: int32(w.id)})
}

func predicateTyp2String(typ cc.PredicateType) string {
	switch typ {
	case cc.EQPREDICATE:
		return "="
	case cc.NEQPREDICATE:
		return "!="
	case cc.GEQPREDICATE:
		return ">="
	case cc.GTPREDICATE:
		return ">"
	case cc.LTPREDICATE:
		return "<"
	case cc.LEQPREDICATE:
		return "<="
	}
	return ""
}

const (
	BatchSize = 256
)

func OpInverse(op string) string {
	switch op {
	case "=":
		return "="
	case "!=":
		return "!="
	case ">":
		return "<"
	case "<":
		return ">"
	case ">=":
		return "<="
	case "<=":
		return ">="
	}
	return ""
}

func (w *Worker) executingPredicate(input map[uint64]*core.Cluster64, operand1, operand2 *cc.Operand, op string) {
	//log.Println(operand1.ToString(), operand2.ToString(), op)
	batchKey := make([]interface{}, 0, BatchSize)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ci := w.ss.ci[operand1.ColumnName]
	cnt := 0
	for iter := ci.Iterator(); iter.HasNext(); {
		k, _ := iter.Next()
		batchKey = append(batchKey, k)
		if len(batchKey) >= BatchSize {
			tmp := make([]string, len(batchKey))
			for i, key := range batchKey {
				tmp[i] = core.ColumnVal2String(key, w.ss.localTable.GetColumnType(operand1.ColumnName))
			}
			results := make([]*core.Cluster64, 0, len(batchKey))
			for i := 0; i < len(batchKey); i++ {
				results = append(results, core.MakeCluster64())
			}

			for i := 0; i < len(w.peers); i++ {
				wg.Add(1)
				if w.id == i {
					go func() {
						ret := w.cbLocalFilter(operand2.ColumnName, OpInverse(op), batchKey)
						mu.Lock()
						defer mu.Unlock()
						for j, cl := range ret {
							results[j].Or(cl)
						}
						wg.Done()
					}()
				} else {
					go func(j int) {
						args := &pb.CBFilterArgs{Table: w.ss.localTable.GetTableName(), ColName: operand2.ColumnName, Op: OpInverse(op), Consts: tmp}
						reply, err := w.peers[j].CBFilter(context.Background(), args)
						if err != nil {
							log.Fatal(err.Error())
						}
						ret := make([]*core.Cluster64, 0, len(reply.Ans))
						for l := 0; l < len(reply.Ans); l++ {
							ret = append(ret, core.MakeCluster64())
							ret[l].FromBytes(reply.Ans[l])
						}
						mu.Lock()
						defer mu.Unlock()
						for l, cl := range ret {
							results[l].Or(cl)
						}
						wg.Done()
					}(i)
				}
			}

			wg.Wait()
			for i, key := range batchKey {
				for cIter := ci.Get(key).Iterator(); cIter.HasNext(); {
					r := cIter.Next()
					input[r].And(results[i])
				}
			}
			batchKey = make([]interface{}, 0, BatchSize)
		}
	}

	if len(batchKey) != 0 {
		cnt += len(batchKey)
		tmp := make([]string, len(batchKey))
		for i, key := range batchKey {
			tmp[i] = core.ColumnVal2String(key, w.ss.localTable.GetColumnType(operand1.ColumnName))
		}
		results := make([]*core.Cluster64, 0, len(batchKey))
		for i := 0; i < len(batchKey); i++ {
			results = append(results, core.MakeCluster64())
		}

		for i := 0; i < len(w.peers); i++ {
			wg.Add(1)
			if w.id == i {
				go func() {
					ret := w.cbLocalFilter(operand2.ColumnName, OpInverse(op), batchKey)
					mu.Lock()
					defer mu.Unlock()
					for j, cl := range ret {
						results[j].Or(cl)
					}
					wg.Done()
				}()
			} else {
				go func(j int) {
					args := &pb.CBFilterArgs{Table: w.ss.localTable.GetTableName(), ColName: operand2.ColumnName, Op: OpInverse(op), Consts: tmp}
					reply, err := w.peers[j].CBFilter(context.Background(), args)
					if err != nil {
						log.Fatal(err.Error())
					}
					ret := make([]*core.Cluster64, 0, len(reply.Ans))
					for l := 0; l < len(reply.Ans); l++ {
						ret = append(ret, core.MakeCluster64())
						ret[l].FromBytes(reply.Ans[l])
					}
					mu.Lock()
					defer mu.Unlock()
					for l, cl := range ret {
						results[l].Or(cl)
					}
					wg.Done()
				}(i)
			}
		}

		wg.Wait()
		for i, key := range batchKey {
			for iter := ci.Get(key).Iterator(); iter.HasNext(); {
				r := iter.Next()
				input[r].And(results[i])
			}
		}
	}
}

func (w *Worker) cbLocalFilter(col string, op string, consts []interface{}) []*core.Cluster64 {
	res := make([]*core.Cluster64, 0, len(consts))
	for i := 0; i < len(consts); i++ {
		res = append(res, core.MakeCluster64())
	}

	ci := w.ss.ci[col]
	switch op {
	case "=":
		for i, c := range consts {
			ret := ci.Get(c)
			if ret != nil {
				res[i].Or(ret)
			}
		}
	case "!=":
		for i, c := range consts {
			ret := ci.Get(c)
			if ret == nil {
				res[i].Or(ret)
			}
		}
	case "<":
		i := 0
		tmp := core.MakeCluster64()
		for iter := ci.Iterator(); iter.HasNext(); {
			key, val := iter.Next()
			k := i
			for ; i < len(consts) && core.Eval(">=", key, consts[i], w.ss.localTable.GetColumnType(col)); i++ {}
			for j := k; j < i; j++ {
				res[j].Or(tmp)
			}
			tmp.Or(val)
		}
		for j := i; j < len(consts); j++ {
			res[j].Or(tmp)
		}
	case "<=":
		i := 0
		tmp := core.MakeCluster64()
		for iter := ci.Iterator(); iter.HasNext(); {
			key, val := iter.Next()
			k := i
			for ; i < len(consts) &&core.Eval(">", key, consts[i], w.ss.localTable.GetColumnType(col)); i++ {}
			for j := k; j < i; j++ {
				res[j].Or(tmp)
			}
			tmp.Or(val)
		}
		for j := i; j < len(consts); j++ {
			res[j].Or(tmp)
		}
	case ">":
		dp := make([]int, 0, ci.Size())
		keys := make([]interface{}, 0, ci.Size())
		i := 0
		for iter := ci.Iterator(); iter.HasNext(); {
			key, _ := iter.Next()
			keys = append(keys, key)
			for ;i < len(consts); i++ {
				if core.Eval(">", key, consts[i], w.ss.localTable.GetColumnType(col)) {
				} else {
					dp = append(dp, i - 1)
					break
				}
			}
			if i == len(consts) {
				dp = append(dp, i - 1)
			}
		}

		tmp := core.MakeCluster64()
		for i = len(dp) - 1; i >= 0; i-- {
			val := ci.Get(keys[i])
			end := -1
			if i != 0 {
				end = dp[i-1]
			}
			tmp.Or(val)
			for j := dp[i]; j > end; j-- {
				res[j].Or(tmp)
			}
		}

	case ">=":
		dp := make([]int, 0, ci.Size())
		keys := make([]interface{}, 0, ci.Size())
		i := 0
		for iter := ci.Iterator(); iter.HasNext(); {
			key, _ := iter.Next()
			keys = append(keys, key)
			for ;i < len(consts); i++ {
				if core.Eval(">=", key, consts[i], w.ss.localTable.GetColumnType(col)) {
				} else {
					dp = append(dp, i - 1)
					break
				}
			}
			if i == len(consts) {
				dp = append(dp, i - 1)
			}
		}

		tmp := core.MakeCluster64()
		for i = len(dp) - 1; i >= 0; i-- {
			val := ci.Get(keys[i])
			end := -1
			if i != 0 {
				end = dp[i-1]
			}
			tmp.Or(val)
			for j := dp[i]; j > end; j-- {
				res[j].Or(tmp)
			}
		}
	}

	return res
}

func (w *Worker) execSingleVarPredicates(input map[uint64]*core.Cluster64, lp *EccDcr.LogicPlan) []cc.Predicate {
	res := make([]cc.Predicate, 0)

	for _, p := range lp.Ps {
		if p.Operand2.Typ != cc.CONSTOPERAND {
			res = append(res, p)
		} else {
			consts := make([]interface{}, 0)
			consts = append(consts, w.ss.localTable.ColumnValTransform(p.Operand2.Val, p.Operand1.ColumnName))
			ret := w.cbLocalFilter(p.Operand1.ColumnName, predicateTyp2String(p.Typ), consts)[0]
			for k := range input {
				if !ret.Contains(k) {
					delete(input, k)
				}
			}
		}
	}

	return res
}

/*
func (w *Worker) rbExecuting() {
	log.Println("start row based executing...")
	us := core.MakeCluster64()
	for k := range w.cds.currPlansInput {
		us.Add(k)
	}

	for _, lp := range w.cds.currentPlans {
		input := w.cds.InputDeepCopy()
		// exec filter predicate
		ps := w.execSingleVarPredicates(input, &lp)

		var wg sync.WaitGroup
		var mu sync.Mutex
		for k, v := range us.Decompose() {
			wg.Add(1)
			if k == w.id {
				go func(p int, rows *core.Cluster) {
					tIter := w.ss.localTable.Iterator()
					for iter := rows.Iterator(); iter.HasNext(); {
						r := iter.Next()
						data := tIter.NextFind(r)
						fs := make([]*core.Filter, len(ps))
						for i := 0; i < len(fs); i++ {
							val := data[w.ss.localTable.GetColumnIndex(ps[i].Operand2.ColumnName)]
							fs[i] = core.MakeFilter(w.ss.localTable.GetTableName(), ps[i].Operand1.ColumnName, predicateTyp2String(ps[i].Typ), w.ss.localTable.ColumnValTransform(val, ps[i].Operand2.ColumnName))
						}
						ret := w.rbLocalFilter(fs, input[core.Encode(p, r)])
						mu.Lock()
						input[core.Encode(p, r)].And(ret)
						mu.Unlock()
					}
					wg.Done()
				} (k, v)
			} else {
				go func(p int, rows *core.Cluster) {
					tIter := w.ss.tmpTables[p].Iterator()
					for iter := rows.Iterator(); iter.HasNext(); {
						r := iter.Next()
						data := tIter.NextFind(r)
						args := &pb.RBFilterArgs{Table: w.ss.localTable.GetTableName(), Op: make([]string, len(ps)), ColName: make([]string, len(ps)), Constant: make([]string, len(ps)), Set: input[core.Encode(p, r)].ToBytes()}
						for i, pd := range ps {
							args.Op[i] = predicateTyp2String(pd.Typ)
							args.ColName[i] = pd.Operand1.ColumnName
							val := data[w.ss.localTable.GetColumnIndex(pd.Operand2.ColumnName)]
							args.Constant[i] = val
						}
						reply, _ := w.peers[p].RBFilter(context.Background(), args)
						ret := core.MakeCluster64()
						ret.FromBytes(reply.Ans)
						mu.Lock()
						input[core.Encode(p, r)].And(ret)
						mu.Unlock()
					}
					wg.Done()
				} (k, v)
			}
		}

		wg.Wait()

		for k, v := range input {
			v.Remove(k)
			if v.Size() == 0 {
				delete(input, k)
			}
		}

		if lp.End != -1 {
			w.cds.finalResults.Put(lp.End, input)
		}

		err := w.cds.interResults.Put(lp.Id, input)
		if err != nil {
			log.Fatal(err.Error())
		}

		cnt := 0
		for _, v := range input {
			cnt += int(v.Size())
		}
		log.Println("result num for logic plan", lp.Id, ":", cnt)

		go func(planId int) {
			reply, _ := w.coordinator.LPComplete(context.Background(), &pb.LPCompleteArgs{MachineId: int32(w.id), PlanId: int32(planId)})
			if reply.CdComplete {
				atomic.StoreInt32(&w.cds.running, 0)
			}
		}(lp.Id)
	}
}
 */

func (w *Worker) BuildClusterIndex(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	atomic.AddInt32(&w.cds.waitClusterIndex, 1)
	return &emptypb.Empty{}, nil
}

func (w *Worker) cbExecuting() {
	log.Println("start executing...")

	cols := make(map[string]bool)
	for _, lp := range w.cds.currentPlans {
		for _, p := range lp.Ps {
			cols[p.Operand1.ColumnName] = true
			if p.Operand2.Typ != cc.CONSTOPERAND {
				cols[p.Operand2.ColumnName] = true
			}
		}
	}

	w.flushTableClusterIndex()

	activeRows := core.MakeCluster64()
	for k := range w.cds.currPlansInput {
		activeRows.Add(k)
	}

	for col := range cols {
		w.buildTableClusterIndex(activeRows, col)
	}

	atomic.AddInt32(&w.cds.waitClusterIndex, 1)
	for i := 0; i < len(w.peers); i++ {
		if i == w.id {
			continue
		}
		go func(j int) {
			w.peers[j].BuildClusterIndex(context.Background(), &emptypb.Empty{})
		} (i)
	}

	for atomic.LoadInt32(&w.cds.waitClusterIndex) != int32(len(w.peers)) {
		time.Sleep(time.Duration(200) * time.Millisecond)
	}

	//log.Println("build cluster index finished...")

	for _, lp := range w.cds.currentPlans {

		log.Println("start executing logic plan", lp)
		input := w.cds.InputDeepCopy()
		ps := w.execSingleVarPredicates(input, &lp)
		for _, p := range ps {
			// log.Println("input size", len(input))
			w.executingPredicate(input, &p.Operand1, &p.Operand2, predicateTyp2String(p.Typ))
		}

		if lp.End != -1 {
			w.cds.dcResults.Put(lp.End, input)
		}

		err := w.cds.interResults.Put(lp.Id, input)
		if err != nil {
			log.Fatal(err.Error())
		}

		cnt := 0
		for _, v := range input {
			cnt += int(v.Size())
		}
		log.Println("result num for logic plan", lp.Id, ":", cnt)

		go func(planId int) {
			reply, _ := w.coordinator.LPComplete(context.Background(), &pb.LPCompleteArgs{MachineId: int32(w.id), PlanId: int32(planId)})
			if reply.CdComplete {
				for k, v := range input {
					v.Remove(k)
					if v.Size() == 0 {
						delete(input, k)
					}
				}
				atomic.StoreInt32(&w.cds.running, 0)
				w.cds.currPlansInput = nil
				w.ss.ci = nil
				w.ss.tmpTables = nil
			}
		}(lp.Id)
	}
}

func (w *Worker) CloseService(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	go func() {
		time.Sleep(time.Duration(1) * time.Second)
		log.Println("close")
		os.Exit(0)
	} ()
	return &emptypb.Empty{}, nil
}

func (w *Worker) CBFilter(ctx context.Context, args *pb.CBFilterArgs) (*pb.FilterReply, error) {
	consts := make([]interface{}, len(args.Consts))
	for i, c := range args.Consts {
		consts[i] = w.ss.localTable.ColumnValTransform(c, args.ColName)
	}
	ret := w.cbLocalFilter(args.ColName, args.Op, consts)
	ans := make([][]byte, len(args.Consts))
	for i, c := range ret {
		ans[i] = c.ToBytes()
	}
	return &pb.FilterReply{Ans: ans}, nil
}

/*
func (w *Worker) RBFilter(ctx context.Context, args *pb.RBFilterArgs) (*pb.FilterReply, error) {
	fs := make([]*core.Filter, len(args.Op))
	for i := 0; i < len(fs); i++ {
		fs[i] = core.MakeFilter(args.Table, args.ColName[i], args.Op[i], w.ss.localTable.ColumnValTransform(args.Constant[i], args.ColName[i]))
	}

	set := core.MakeCluster64()
	set.FromBytes(args.Set)
	ans := w.rbLocalFilter(fs, set)
	return &pb.FilterReply{Ans: ans.ToBytes()}, nil
}

 */

/*
func (w *Worker) rbLocalFilter(fs []*core.Filter, set *core.Cluster64) *core.Cluster64 {
	res := core.MakeCluster64()

	var wg sync.WaitGroup
	var mu sync.Mutex
	for k, v := range set.Decompose() {
		wg.Add(1)
		go func(j int, u *core.Cluster) {
			if j == w.id {
				ret := w.ss.localTable.SearchInSubsetByFilter(u, fs...)
				mu.Lock()
				res.BatchAdd(j, ret)
				mu.Unlock()
			} else {
				table, ok := w.ss.tmpTables[j]
				if ok {
					ret := table.SearchInSubsetByFilter(u, fs...)
					mu.Lock()
					res.BatchAdd(j, ret)
					mu.Unlock()
				}
			}
			wg.Done()
		} (k, v)
	}

	wg.Wait()

	return res
}

 */


func (w *Worker) SinceInferDate(ctx context.Context, args *pb.SinceInferDateArgs) (*pb.SinceInferDateReply, error) {
	tempoCol := w.ss.localTable.GetTemporalColumnName()
	mp := w.ss.localTable.BuildClusterIndex(tempoCol)
	reply := &pb.SinceInferDateReply{Dates: make([]int64, 0)}

	for k := range mp {
		t := k.(time.Time).Unix()
		if t >= args.FromDate && t <= args.ToDate {
			reply.Dates = append(reply.Dates, t)
		}
	}

	return reply, nil
}

func (w *Worker) SinceInfer(ctx context.Context, args *pb.SinceInferArgs) (*emptypb.Empty, error) {
	go w.sinceInfer(int(args.Dc), args.Dates, args.Direct)
	return &emptypb.Empty{}, nil
}

func (w *Worker) AcquireTime(ctx context.Context, args *pb.AcquireTimeArgs) (*pb.AcquireTimeReply, error) {
	c := core.MakeCluster()
	c.FromBytes(args.Cluster)
	res := &pb.AcquireTimeReply{Times: w.getTime(c)}
	return res, nil
}

func (w *Worker) ResultPersist(ctx context.Context, args *pb.ResultPersistArgs) (*emptypb.Empty, error) {
	go w.resultPersist(int(args.Dc), args.Time, args.Direct)

	return &emptypb.Empty{}, nil
}

func (w *Worker) resultPersist(dc int, time int64, direct bool) {
	if time == (1 << 63 - 1) {
		conflict := w.cds.dcResults.Get(dc)
		w.cds.tdcResults.Put(dc, conflict)
	} else {
		conflict := w.cds.dcResults.Get(dc)
		result := make(map[uint64]*core.Cluster64)
		if direct == false {
			for _, t := range w.cds.sinceInferTimeKeys {
				if t >= time {
					for _, uv := range w.cds.sinceInferConflict[t] {
						if _, ok := conflict[uv[0]]; ok {
							if conflict[uv[0]].Contains(uv[1]) {
								if _, ok := result[uv[0]]; !ok {
									result[uv[0]] = core.MakeCluster64()
								}
								result[uv[0]].Add(uv[1])
							}
						}
					}
				}
			}
		} else {
			for _, t := range w.cds.sinceInferTimeKeys {
				if t < time {
					for _, uv := range w.cds.sinceInferConflict[t] {
						if _, ok := conflict[uv[0]]; ok {
							if conflict[uv[0]].Contains(uv[1]) {
								if _, ok := result[uv[0]]; !ok {
									result[uv[0]] = core.MakeCluster64()
								}
								result[uv[0]].Add(uv[1])
							}
						}
					}
				}
			}
		}

		w.cds.tdcResults.Put(dc, result)
	}
	log.Println("finish result merge of dc id", dc)
	w.coordinator.ResultPersistComplete(context.Background(), &emptypb.Empty{})
}

func (w *Worker) sinceInfer(dc int, dates []int64, direct bool) {
	conflicts := w.cds.dcResults.Get(dc)

	tempoMap := make(map[uint64]int64)
	rows := core.MakeCluster64()
	for k, v := range conflicts {
		rows.Add(k)
		for x, y := range v.Decompose() {
			rows.BatchAdd(x, y)
		}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	for wid, c := range rows.Decompose() {
		wg.Add(1)
		if wid == w.id {
			ret := w.getTime(c)
			k := 0
			mu.Lock()
			for iter := c.Iterator(); iter.HasNext(); {
				r := iter.Next()
				tempoMap[core.Encode(wid, r)] = ret[k]
				k++
			}
			mu.Unlock()
			wg.Done()
		} else {
			go func(j int) {
				reply, _ := w.peers[j].AcquireTime(context.Background(), &pb.AcquireTimeArgs{Cluster: c.ToBytes()})
				k := 0
				mu.Lock()
				for iter := c.Iterator(); iter.HasNext(); {
					r := iter.Next()
					tempoMap[core.Encode(j, r)] = reply.Times[k]
				}
				wg.Done()
			} (wid)
		}
	}

	wg.Wait()

	w.cds.sinceInferConflict = make(map[int64][][2]uint64)

	for u, vCluster := range conflicts {
		for iter := vCluster.Iterator(); iter.HasNext(); {
			v := iter.Next()

			t := myutil.MaxInt64(tempoMap[u], tempoMap[v])
			if _, ok := w.cds.sinceInferConflict[t]; !ok {
				w.cds.sinceInferConflict[t] = make([][2]uint64, 0)
			}
			w.cds.sinceInferConflict[t] = append(w.cds.sinceInferConflict[t], [2]uint64{u, v})
		}
	}

	w.cds.sinceInferTimeKeys = make([]int64, 0, len(w.cds.sinceInferConflict))
	for k := range w.cds.sinceInferConflict {
		w.cds.sinceInferTimeKeys = append(w.cds.sinceInferTimeKeys, k)
	}

	sort.Slice(w.cds.sinceInferTimeKeys, func(i, j int) bool {
		return w.cds.sinceInferTimeKeys[i] < w.cds.sinceInferTimeKeys[j]
	})

	acc := make([]int32, len(dates))
	if !direct { // acc(dc1, >=, t)
		for _, t := range w.cds.sinceInferTimeKeys {
			i := sort.Search(len(dates), func(i int) bool {
				return t >= dates[i]
			})
			if i != len(dates) {
				acc[i] += int32(len(w.cds.sinceInferConflict[t]))
			}
		}

		for i := len(acc) - 2; i >= 0; i-- {
			acc[i] = acc[i] + acc[i+1]
		}
	} else {
		for _, t := range w.cds.sinceInferTimeKeys {
			i := sort.Search(len(dates), func(i int) bool {
				return t < dates[i]
			})
			if i != len(dates) {
				acc[i] += int32(len(w.cds.sinceInferConflict[t]))
			}
		}

		for i := 1; i < len(acc); i++ {
			acc[i] = acc[i] + acc[i-1]
		}
	}

	w.coordinator.SinceTimeAcc(context.Background(), &pb.SinceTimeAccArgs{Id: int32(dc), Acc: acc})
}

func (w *Worker) getTime(c *core.Cluster) []int64 {
	res := make([]int64, 0)

	colVals := w.ss.localTable.GetColumnVals(c, w.ss.localTable.GetTemporalColumnName())
	for _, t := range colVals {
		res = append(res, t.(time.Time).Unix())
	}
	return res
}

