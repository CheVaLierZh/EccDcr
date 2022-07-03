package main

import (
	"EccDcr"
	"EccDcr/core"
	"EccDcr/myutil"
	"EccDcr/pb"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Worker struct {
	id          int

	ss          *StorageService

	peers       []pb.WorkerClient
	peersConn []*grpc.ClientConn
	coordinator pb.CoordinatorClient
	coordinatorConn *grpc.ClientConn

	cds         *ConflictDetectService
	rs *RepairService
	es *EvaluateService
	pb.UnimplementedWorkerServer

	port string
}

func (w *Worker) CloseConflictDetect() {
	w.cds = nil
}

func (w *Worker) CloseRepairService() {
	w.rs = nil
}

func (w *Worker) CloseEvaluateService() {
	w.es = nil
}

type StorageService struct {
	dataDir           string
	localTable        *core.CsvTable
	tmpTables         map[int]*core.TbTable
	tmpDir string
	ci map[string]*ClusterIndex
	mu sync.Mutex
}

type ClusterIndex struct {
	mp map[interface{}]*core.Cluster64
	keyTyp core.ColumnType
}

func MakeClusterIndex(mp map[interface{}]*core.Cluster64, keyTyp core.ColumnType) *ClusterIndex {
	res := &ClusterIndex{mp: mp, keyTyp: keyTyp}
	return res
}

func (ci *ClusterIndex) Get(key interface{}) *core.Cluster64 {
	if _, ok := ci.mp[key]; !ok {
		return nil
	} else {
		return ci.mp[key]
	}
}

func (ci *ClusterIndex) Size() int {
	return len(ci.mp)
}

func (ci *ClusterIndex) ToMap() map[interface{}]*core.Cluster64 {
	return ci.mp
}

type ClusterIndexIterator struct {
	idx int
	ci *ClusterIndex
	keys []interface{}
	direct bool // true iff reverse iterator
}

func (ci *ClusterIndex) Iterator() *ClusterIndexIterator {
	res := &ClusterIndexIterator{idx: -1, ci: ci}
	res.keys = make([]interface{}, 0, len(ci.mp))
	for k := range ci.mp {
		res.keys = append(res.keys, k)
	}
	core.SliceSort(res.keys, res.ci.keyTyp)
	res.direct = false
	return res
}

func (iter *ClusterIndexIterator) HasNext() bool {
	if !iter.direct {
		return iter.idx+1 < len(iter.keys)
	} else {
		return iter.idx - 1 >= 0
	}
}

func (iter *ClusterIndexIterator) Next() (interface{}, *core.Cluster64) {
	if !iter.direct {
		iter.idx++
	} else {
		iter.idx--
	}
	k := iter.keys[iter.idx]
	return k, iter.ci.mp[k]
}

func (ci *ClusterIndex) ReverseIterator() *ClusterIndexIterator {
	res := &ClusterIndexIterator{idx: len(ci.mp), ci: ci}
	res.keys = make([]interface{}, 0, len(ci.mp))
	for k := range ci.mp {
		res.keys = append(res.keys, k)
	}
	core.SliceSort(res.keys, res.ci.keyTyp)
	res.direct = true
	return res
}

func (w *Worker) flushTableClusterIndex() {
	w.ss.ci = make(map[string]*ClusterIndex)
}

func(w *Worker) buildTableClusterIndex(c *core.Cluster64, col string) {
	res := make(map[interface{}]*core.Cluster64)

	ret := w.ss.localTable.BuildClusterIndex(col)
	for k, v := range ret {
		if _, ok := res[k]; !ok {
			res[k] = core.MakeCluster64()
		}
		res[k].BatchAdd(w.id, v)
	}
	for m, t := range w.ss.tmpTables {
		ret = t.BuildClusterIndex(col)
		for k, v := range ret {
			if _, ok := res[k]; !ok {
				res[k] = core.MakeCluster64()
			}
			res[k].BatchAdd(m, v)
		}
	}

	cnt := uint64(0)
	for _, v := range res {
		v.And(c)
		cnt += v.Size() * v.Size()
	}

	//log.Println(col, "key size", len(res), "val size", cnt)
	w.ss.ci[col] = MakeClusterIndex(res, w.ss.localTable.GetColumnType(col))
}

func (w *Worker) getTableClusterIndex(col string) *ClusterIndex {
	return w.ss.ci[col]
}



func MakeStorageService(dataDir string) *StorageService {
	return &StorageService{dataDir: dataDir}
}

func (ss *StorageService) Initialize(table string, tableMeta map[string]core.ColumnType, sep string) error {
	log.Println("start init from table " + table)
	localTable, err := core.MakeCsvTable(tableMeta, table, filepath.Join(ss.dataDir, table + ".csv"), sep)
	if err != nil {
		return err
	}
	ss.localTable = localTable
	ss.tmpTables = make(map[int]*core.TbTable)
	ss.tmpDir = filepath.Join(myutil.ExecPath(), "tables")
	if ok, _ := myutil.PathExists(ss.tmpDir); !ok {
		err = os.Mkdir(ss.tmpDir, os.ModePerm)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
	log.Println("local table size", ss.localTable.Size())
	return nil
}

func (ss *StorageService) TmpTablesFlush() {
	ss.tmpTables = make(map[int]*core.TbTable)
}

func (ss *StorageService) CreateTmpTable(machine int) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if _, ok := ss.tmpTables[machine]; !ok {
		fileName := filepath.Join(ss.tmpDir, fmt.Sprintf("tmp-%d.tb", machine))
		ss.tmpTables[machine] = ss.localTable.MakeTmpTable(fileName)
	}
}

func (ss *StorageService) Close() {
	os.RemoveAll(ss.tmpDir)
}

func (w *Worker) Server() {
	lis, err := net.Listen("tcp", ":" + w.port)
	if err != nil {
		log.Fatal("failed to listen on port " + w.port)
	}
	server := grpc.NewServer()
	pb.RegisterWorkerServer(server, w)
	server.Serve(lis)
}

func MakeWorker(coordinatorIp string, port int, dataDir string) *Worker {
	if port < 0 || port > 65535 {
		log.Fatal("invalid port", port)
	}

	res := &Worker{}
	res.port = strconv.Itoa(port)
	conn, err := grpc.Dial(coordinatorIp + EccDcr.CLOUD_PORT, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err.Error())
	}
	res.coordinatorConn = conn
	res.coordinator = pb.NewCoordinatorClient(res.coordinatorConn)

	args := &pb.RegisterArgs{Address: res.port}
	reply, err := res.coordinator.Register(context.Background(), args)
	if err != nil {
		log.Fatal("fail to connect coordinator")
	}
	res.id = int(reply.Id)
	log.Println("worker id", res.id)

	res.ss = MakeStorageService(dataDir)

	return res
}

const (
	MAXGRPCSIZE = 1024 * 1024 * 16
)

func (w *Worker) TableCheck(ctx context.Context, args *pb.TableCheckArgs) (*pb.TableCheckReply, error) {
	reply := &pb.TableCheckReply{Valid: false}
	log.Println("start doing table check...")

	tableMeta := make(map[string]core.ColumnType)
	for k, v := range args.ColumnsType {
		tableMeta[k] = core.IntToColumnType(int(v))
	}
	err := w.ss.Initialize(args.Table, tableMeta, args.Sep)
	if err != nil {
		log.Println(err.Error())
	}
	ok := true
	columnsTyp := w.ss.localTable.GetColumnsType()
	for k, v := range args.ColumnsType {
		if typ, exist := columnsTyp[k]; !exist || int32(typ) != v {
			ok = false
			log.Println("col type not exist or match", k)
			break
		}
	}

	if err == nil && ok {
		reply.Valid = true
		w.peers = make([]pb.WorkerClient, len(args.Peers))
		w.peersConn = make([]*grpc.ClientConn, len(args.Peers))
		for i, address := range args.Peers {
			if i != w.id {
				w.peersConn[i], err = grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MAXGRPCSIZE), grpc.MaxCallSendMsgSize(MAXGRPCSIZE)))
				if err != nil {
					log.Fatal(err.Error())
				}
				w.peers[i] = pb.NewWorkerClient(w.peersConn[i])
			}
		}
	}

	go w.DoHeartbeat()

	return reply, nil
}

func (w *Worker) ColumnStatis(ctx context.Context, args *pb.ColumnStatisArgs) (*pb.ColumnStatisReply, error) {
	reply := &pb.ColumnStatisReply{}
	data, _ := w.ss.localTable.GetColumnHistogram(args.Column).ToBytes()
	reply.Histogram = data
	return reply, nil
}

func (w *Worker) DoHeartbeat() {
	for {
		_, err := w.coordinator.Heartbeat(context.Background(), &pb.HeartbeatArgs{Id: int32(w.id)})
		if err != nil {
			log.Println("fail to connect coordinator")
			os.Exit(1)
		}
		time.Sleep(time.Duration(200) * time.Millisecond)
	}
}