package main

import (
	"EccDcr"
	"EccDcr/cc"
	"EccDcr/core"
	"EccDcr/estimating"
	"EccDcr/myutil"
	"EccDcr/pb"
	"context"
	"errors"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	PHASEMAXNODES int = 3
)

type ConflictDetectManager struct {
	tdc2dc map[int][]int
	progress         *EccDcr.PhaseProgess
	parentPlanId     int
	currentPlans     []EccDcr.LogicPlan
	lpHasDone int32
	balanceHasDone int32
	running int32
	rowBased bool

	tmpDcAcc []int32
	tmpDcHasDone int32
	resultPersistHasDone int32

	mu sync.Mutex
}

func (cdm *ConflictDetectManager) PlansHasNext() bool {
	return cdm.progress.HasNext()
}

func (cdm *ConflictDetectManager) PlansNext() (int, []EccDcr.LogicPlan, float64) {
	return cdm.progress.Next()
}

func histogramFromBytes(typ string, data []byte) (estimating.Histogram, error) {
	switch typ {
	case "Optimal":
		hist := &estimating.OptimalHistogram{}
		err := hist.FromBytes(data)
		return hist, err
	case "Maxdiff":
		hist := &estimating.MaxdiffHistogram{}
		err := hist.FromBytes(data)
		return hist, err
	case "Endbiased":
		hist := &estimating.EndbiasedHistogram{}
		err := hist.FromBytes(data)
		return hist, err
	}

	return nil, errors.New("can not find histogram type " + typ)
}

func predicateTypeTransform(typ cc.PredicateType) estimating.EstimateType {
	res := estimating.EQ
	switch typ {
	case cc.EQPREDICATE:
		res = estimating.EQ
	case cc.NEQPREDICATE:
		res = estimating.NEQ
	case cc.GTPREDICATE:
		res = estimating.GT
	case cc.GEQPREDICATE:
		res = estimating.GEQ
	case cc.LEQPREDICATE:
		res = estimating.LEQ
	case cc.LTPREDICATE:
		res = estimating.LT
	}
	return res
}

func predicateVarOperand(p *cc.Predicate) int {
	if p.Operand2.Typ == cc.CONSTOPERAND {
		return 1
	} else {
		return 0
	}
}

func (c *Coordinator) MakeConflictDetectManager(tdcs []*cc.TemporalDC, table string) {
	c.cdm = nil

	res := &ConflictDetectManager{}
	res.parentPlanId = -1
	res.running = 0
	res.tdc2dc = make(map[int][]int)

	// decompose tdc to dc
	dcs := make([]*cc.DenialConstraint, 0)
	dcBelongs := make([]int, 0)

	tablesTemporalAttr := make(map[string]string)
	for t, meta := range c.tablesMeta {
		for col, typ := range meta {
			if typ == core.TIME {
				tablesTemporalAttr[t] = col
			}
		}
	}

	for i, tdc := range tdcs {
		splitDc := make([]int, 0)
		for _, dc := range tdc.ToDCs(tablesTemporalAttr) {
			splitDc = append(splitDc, len(dcs))
			dcs = append(dcs, dc)
			dcBelongs = append(dcBelongs, i)
		}
		res.tdc2dc[i] = splitDc
	}

	// collect table statistics
	statis := make(map[string][]estimating.Histogram)
	for colName := range c.tablesMeta[table] {
		statis[colName] = make([]estimating.Histogram, len(c.followers))
		var wg sync.WaitGroup
		for i := 0; i < len(c.followers); i++ {
			wg.Add(1)
			go func(j int) {
				args := &pb.ColumnStatisArgs{Column: colName}
				ctx := context.Background()
				reply, _ := c.followers[j].ColumnStatis(ctx, args)
				if c.tablesMeta[table][colName] == core.STRING {
					statis[colName][j], _ = histogramFromBytes(core.STRINGHISTOGRAMTYPE, reply.Histogram)
				} else {
					statis[colName][j], _ = histogramFromBytes(core.NUMERICHISTOGRAMTYPE, reply.Histogram)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	}

	// dc decompose to predicate set
	pFreq := make(map[cc.Predicate]int)
	pOwners := make(map[cc.Predicate]map[int]bool)
	for i, dc := range dcs {
		for _, p := range dc.Ps {
			pFreq[*p] += 1
			if _, ok := pOwners[*p]; !ok {
				pOwners[*p] = make(map[int]bool)
			}
			pOwners[*p][i] = true
		}
	}

	// generate predicate tree nodes
	nodes := make([]*EccDcr.TreeNode, 0)
	for p, f := range pFreq {
		sel := 0.0
		switch predicateVarOperand(&p) {
		case 0:
			sel = computePredicateSelectivity(statis[p.Operand1.ColumnName], statis[p.Operand2.ColumnName], predicateTypeTransform(p.Typ))
		case 1:
			sel = computeRangeSelectivity(statis[p.Operand1.ColumnName], p.Operand2.Val, predicateTypeTransform(p.Typ))
		}

		owners := make([]int, 0)
		for k := range pOwners[p] {
			owners = append(owners, k)
		}

		cpy := p
		nodes = append(nodes, EccDcr.MakeTreeNode(&cpy, sel, float64(f), owners))
	}

	tree := EccDcr.MakePredicateTree(nodes, len(dcs))
	res.progress = EccDcr.MakePhaseProgress(tree.BuildPhases(PHASEMAXNODES))

	c.cdm = res
}

func (cdm *ConflictDetectManager) cbSharding(distribution []int) [][]int {
	res := make([][]int, len(distribution))
	avg := 0
	for i := 0; i < len(distribution); i++ {
		avg += distribution[i]
		res[i] = make([]int, len(distribution))
	}
	avg = (avg / len(distribution)) + 1
	for true {
		maxIdx := -1
		minIdx := -1
		min := 1<<31 - 1
		max := 0
		for i := 0; i < len(distribution); i++ {
			if distribution[i] > max {
				maxIdx = i
				max = distribution[i]
			}
			if distribution[i] < min {
				minIdx = i
				min = distribution[i]
			}
		}
		if max > avg {
			diff := myutil.MinInt(max-avg, avg-min)
			distribution[minIdx] += diff
			distribution[maxIdx] -= diff
			res[maxIdx][minIdx] = diff
		} else {
			break
		}
	}
	return res
}

func (cdm *ConflictDetectManager) rbSharding(m int, distrib map[uint64]int) map[uint64]int {
	res := make(map[uint64]int)
	tasks := make([][2]uint64, 0, len(distrib))
	for k, v := range distrib {
		tasks = append(tasks, [2]uint64{k, uint64(v)})
	}
	sort.Slice(tasks, func(i,j int) bool {
		return tasks[i][1] > tasks[j][1]
	})

	load := make([]int, m)
	for _, t := range tasks {
		minLoad := 1 << 31 - 1
		min := -1
		e, _ := core.Decode(t[0])
		for i, l := range load {
			if l < minLoad || (l == minLoad && i == e) {
				minLoad = l
				min = i
			}
		}
		res[t[0]] = min
		load[min] += int(t[1])
	}

	return res
}

func computePredicateSelectivity(x []estimating.Histogram, y []estimating.Histogram, typ estimating.EstimateType) float64 {
	size := 0.0
	xTotal := 0.0
	for _, h := range x {
		xTotal += h.Count()
	}
	yTotal := 0.0
	for _, h := range y {
		yTotal += h.Count()
	}

	for _, a := range x {
		for _, b := range y {
			size += a.PredicateEstimate(typ, b)
		}
	}
	return size / (xTotal * yTotal)
}

func computeRangeSelectivity(histograms []estimating.Histogram, c interface{}, typ estimating.EstimateType) float64 {
	total := 0.0
	for _, h := range histograms {
		total += h.Count()
	}

	size := 0.0
	if d, ok := c.(time.Time); ok {
		c = float64(d.Unix())
	}
	for _, h := range histograms {
		size += h.RangeEstimate(typ, c)
	}

	res := size / total
	return res * res
}

func (c *Coordinator) LPComplete(ctx context.Context, _ *pb.LPCompleteArgs) (*pb.LPCompleteReply, error) {
	reply := &pb.LPCompleteReply{CdComplete: false}

	hasDone := atomic.AddInt32(&c.cdm.lpHasDone, 1)
	if hasDone == int32(len(c.followers)*len(c.cdm.currentPlans)) {
		if !c.cdm.PlansHasNext() {
			reply.CdComplete = true
			atomic.StoreInt32(&c.cdm.running, 0)
		}
	}

	return reply, nil
}

const (
	SelThreshold = -1
)

func (c *Coordinator) balancing() {
	c.cdm.balanceHasDone = 0
	var sel float64
	c.cdm.parentPlanId, c.cdm.currentPlans, sel = c.cdm.PlansNext()
	log.Println("start balancing for plans", c.cdm.currentPlans, "with parentId", c.cdm.parentPlanId)
	if len(c.cdm.currentPlans) != 0 {
		plansByte := make([][]byte, len(c.cdm.currentPlans))
		for i := 0; i < len(c.cdm.currentPlans); i++ {
			bytes, _ := c.cdm.currentPlans[i].ToBytes()
			plansByte[i] = bytes
		}

		if sel > SelThreshold {
			//log.Println("start column based...")
			c.cdm.rowBased = false

			distrib := make([]int, len(c.followers))
			// call for worker input balance arguments
			var wg sync.WaitGroup
			for i := 0; i < len(c.followers); i++ {
				wg.Add(1)
				go func(j int) {
					args := &pb.BalanceArgs{ParentPlanId: int32(c.cdm.parentPlanId)}
					reply, _ := c.followers[j].CBBalance(context.Background(), args)
					distrib[j] = int(reply.WorkLoad)
					wg.Done()
				}(i)
			}
			wg.Wait()

			balancing := c.cdm.cbSharding(distrib)
			// push balance strategy to workers
			for i := 0; i < len(c.followers); i++ {
				go func(j int) {
					delivery := make([]int32, len(c.followers))
					for k := 0; k < len(c.followers); k++ {
						delivery[k] = int32(balancing[j][k])
						//log.Println(j, "send balance workload to", k, "size:", delivery[k])
					}
					args := &pb.CBBalanceWorkloadArgs{Delivery: delivery}
					c.followers[j].CBBalanceWorkload(context.Background(), args)
				}(i)
			}
		}
		/* else {
			log.Println("start row based...")
			c.cdm.rowBased = true

			distrib := make(map[uint64]int)
			from := make(map[uint64]int)

			var wg sync.WaitGroup
			var mu sync.Mutex
			for i := 0; i < len(c.followers); i++ {
				wg.Add(1)
				go func(j int) {
					args := &pb.BalanceArgs{ParentPlanId: int32(c.cdm.parentPlanId)}
					reply, _ := c.followers[j].RBBalance(context.Background(), args)
					mu.Lock()
					defer mu.Unlock()
					for k, v := range reply.Distrib {
						distrib[k] += int(v)
						from[k] = j
					}
					wg.Done()
				}(i)
			}
			wg.Wait()

			balancing := c.cdm.rbSharding(len(c.followers), distrib)
			allocate := make(map[int]map[uint64]int)
			for k, v := range balancing {
				if from[k] == v {
					continue
				}
				if _, ok := allocate[from[k]]; !ok {
					allocate[from[k]] = make(map[uint64]int)
				}
				allocate[from[k]][k] = v
			}
			log.Println("row based complete sharding computing")
			for i := 0; i < len(c.followers); i++ {
				go func(j int) {
					args := &pb.RBBalanceWorkloadArgs{Allocate: make(map[uint64]int32)}
					for k, v := range allocate[j] {
						args.Allocate[k] = int32(v)
					}
					log.Println("send balance workload to", j, "size:", len(args.Allocate))
					_, err := c.followers[j].RBBalanceWorkload(context.Background(), args)
					if err != nil {
						log.Fatal(err.Error())
					}
				}(i)
			}
		}

		*/
	}
}

func (c *Coordinator) executing() {
	c.cdm.lpHasDone = 0
	log.Println("start executing for plans with parentPlanId", c.cdm.parentPlanId)
	for i := 0; i < len(c.followers); i++ {
		go func(j int) {
			plans := make([][]byte, len(c.cdm.currentPlans))
			for k, plan := range c.cdm.currentPlans {
				plans[k], _ = plan.ToBytes()
			}
			if !c.cdm.rowBased {
				args := &pb.PushLPArgs{Plans: plans}
				c.followers[j].PushCBLP(context.Background(), args)
			} else {
				args := &pb.PushLPArgs{Plans: plans}
				c.followers[j].PushRBLP(context.Background(), args)
			}
		}(i)
	}
}

func (c *Coordinator) BalanceComplete(ctx context.Context, args *pb.BalanceCompleteArgs) (*emptypb.Empty, error) {
	atomic.AddInt32(&c.cdm.balanceHasDone, 1)
	//log.Println("machine", args.MachineId, "complete balancing")
	return &emptypb.Empty{}, nil
}

func (c *Coordinator) ResultPersistComplete(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	atomic.AddInt32(&c.cdm.resultPersistHasDone, 1)
	return &emptypb.Empty{}, nil
}

func (c *Coordinator) ConflictDetect() {
	log.Println("start doing conflict detect...")

	c.MakeConflictDetectManager(c.workingTDCs, c.workingTable)

	atomic.StoreInt32(&c.cdm.running, 1)
	t1 := time.Now()
	for atomic.LoadInt32(&c.cdm.running) == int32(1) {
		start := time.Now()

		c.balancing()
		for atomic.LoadInt32(&c.cdm.balanceHasDone) != int32(len(c.followers)) { // wait for balance part complete
			time.Sleep(time.Duration(500) * time.Millisecond)
		}

		balance := time.Now()
		log.Println("this turn balance time:", balance.Sub(start).Seconds(), "s")
		c.executing()
		for atomic.LoadInt32(&c.cdm.lpHasDone) != int32(len(c.cdm.currentPlans)*len(c.followers)) {
			time.Sleep(time.Duration(500) * time.Millisecond)
		}
		exec := time.Now()
		log.Println("this turn exec time:", exec.Sub(balance).Seconds(), "s")
		log.Println("this turn cost time:", exec.Sub(start).Seconds(), "s")
	}

	c.ResultMerge()

	log.Println("conflict detect cost time:", time.Now().Sub(t1).Seconds(), "s")

	total := 0
	distrib := make(map[int]int)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < len(c.followers); i++ {
		wg.Add(1)
		go func(j int) {
			reply, _ := c.followers[j].ConflictDetectResult(context.Background(), &emptypb.Empty{})
			mu.Lock()
			for k := 0; k < len(reply.DcId); k++ {
				distrib[int(reply.DcId[k])] += int(reply.CCnt[k])
			}
			total += int(reply.Total)
			mu.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()

	log.Println("total conflict num: ", total)
}

func (c *Coordinator) SinceMerge(id int, tdc *cc.TemporalDC) {

	datesMap := make(map[int64]bool)
	var mu sync.Mutex
	for i := 0; i < len(c.followers); i++ {
		go func(j int) {
			reply, _ := c.followers[j].SinceInferDate(context.Background(), &pb.SinceInferDateArgs{FromDate: myutil.Time2Int(tdc.FromDate), ToDate: myutil.Time2Int(tdc.ToDate)})
			mu.Lock()
			defer mu.Unlock()
			for _, t := range reply.Dates {
				datesMap[t] = true
			}
		} (i)
	}

	dates := make([]int64, 0, len(datesMap))
	for k := range datesMap {
		dates = append(dates, k)
	}

	if len(dates) >= 100000 {
		log.Fatal("too many time points in dataset!")
	}

	sort.Slice(dates, func(i, j int) bool {
		return dates[i] < dates[j]
	})

	dcs := c.cdm.tdc2dc[id]

	c.cdm.tmpDcHasDone = 0
	c.cdm.tmpDcAcc = make([]int32, len(dates))

	// get dc1Acc
	for i := 0; i < len(c.followers); i++ {
		go func(j int) {
			c.followers[j].SinceInfer(context.Background(), &pb.SinceInferArgs{Dc: int32(dcs[0]), Dates: dates, Direct: false}) // true means acc(dc2, <, t) false means acc(dc1, >=, t)
		} (i)
	}

	for atomic.LoadInt32(&c.cdm.tmpDcHasDone) != int32(len(c.followers)) {
		time.Sleep(time.Duration(1) * time.Second)
	}
	dc1Acc := c.cdm.tmpDcAcc

	c.cdm.tmpDcHasDone = 0
	c.cdm.tmpDcAcc = make([]int32, len(dates))

	// get dc2Acc
	for i := 0; i < len(c.followers); i++ {
		go func(j int) {
			c.followers[j].SinceInfer(context.Background(), &pb.SinceInferArgs{Dc: int32(dcs[1]), Dates: dates, Direct: true})
		} (i)
	}

	dc2Acc := c.cdm.tmpDcAcc

	timePoints := make([]time.Time, 0, len(dates))
	for _, t := range dates {
		timePoints = append(timePoints, *myutil.Int2Time(t))
	}

	inferTime := tdc.SinceInfer(timePoints, dc1Acc, dc2Acc)
	var wg sync.WaitGroup
	for i := 0; i < len(c.followers); i++ {
		wg.Add(1)
		go func(j int) {
			c.followers[j].ResultPersist(context.Background(), &pb.ResultPersistArgs{Dc: int32(dcs[0]), Time: inferTime.Unix(), Direct: false})
			wg.Done()
		} (i)
	}
	wg.Wait()

	for i := 0; i < len(c.followers); i++ {
		wg.Add(1)
		go func(j int) {
			c.followers[j].ResultPersist(context.Background(), &pb.ResultPersistArgs{Dc: int32(dcs[1]), Time: inferTime.Unix(), Direct: true})
			wg.Done()
		} (i)
	}
	wg.Wait()
}


func (c *Coordinator) ResultMerge() {
	log.Println("start result merge...")
	for tdcId, tdc := range c.workingTDCs {
		c.cdm.resultPersistHasDone = 0
		if tdc.GetType() == cc.SINCE {
			c.SinceMerge(tdcId, tdc)
		} else {
			var wg sync.WaitGroup
			for i := 0; i < len(c.followers); i++ {
				wg.Add(1)
				go func(j int) {
					for _, dcId := range c.cdm.tdc2dc[tdcId] {
						c.followers[j].ResultPersist(context.Background(), &pb.ResultPersistArgs{Dc: int32(dcId), Time: 1 << 63 - 1, Direct: false})
					}
					wg.Done()
				} (i)
			}
			wg.Wait()
		}
		for atomic.LoadInt32(&c.cdm.resultPersistHasDone) != int32(len(c.followers)) {
			time.Sleep(time.Duration(200) * time.Millisecond)
		}
	}
}

func (c *Coordinator) SinceTimeAcc(ctx context.Context, args *pb.SinceTimeAccArgs) (*emptypb.Empty, error) {
	atomic.AddInt32(&c.cdm.tmpDcHasDone, 1)
	c.cdm.mu.Lock()
	defer c.cdm.mu.Unlock()

	for i := 0; i < len(args.Acc); i++ {
		c.cdm.tmpDcAcc[i] += args.Acc[i]
	}
	return &emptypb.Empty{}, nil
}