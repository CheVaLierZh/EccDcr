package main

import (
	"EccDcr"
	"EccDcr/cc"
	"EccDcr/core"
	"EccDcr/pb"
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	mu           sync.Mutex

	followers     []pb.WorkerClient
	followersConn []*grpc.ClientConn
	followersAddr []string

	tablesMeta   map[string]map[string]core.ColumnType
	tablesSep    map[string]string

	workingTDCs []*cc.TemporalDC
	workingTable string

	heartbeat map[int]time.Time

	cdm          *ConflictDetectManager
	rm           *RepairManager
	em           *EvaluateManager

	pb.UnimplementedCoordinatorServer
}

func (c *Coordinator) CloseConflictDetectManager() {
	c.cdm = nil
}

func (c *Coordinator) CloseRepairManager() {
	c.rm = nil
}

func (c *Coordinator) CloseEvaluateManager() {
	c.em = nil
}

func (c *Coordinator) parseMeta(meta map[string]map[string]string) {
	c.tablesMeta = make(map[string]map[string]core.ColumnType)
	c.tablesSep = make(map[string]string)
	for t, m := range meta {
		c.tablesMeta[t] = make(map[string]core.ColumnType)
		for k, v := range m {
			if k == "@Sep" {
				c.tablesSep[t] = v
			} else {
				switch v {
				case "LONG":
					c.tablesMeta[t][k] = core.LONG
				case "NUMERIC":
					c.tablesMeta[t][k] = core.NUMERIC
				case "TIME":
					c.tablesMeta[t][k] = core.TIME
				case "STRING":
					c.tablesMeta[t][k] = core.STRING
				}
			}
		}
	}
}

func MakeCoordinator(meta []byte) *Coordinator {
	res := &Coordinator{}
	res.followers = make([]pb.WorkerClient, 0)
	res.followersConn = make([]*grpc.ClientConn, 0)
	res.followersAddr = make([]string, 0)
	var tmp map[string]map[string]string
	err := json.Unmarshal(meta, &tmp)
	res.parseMeta(tmp)

	res.heartbeat = make(map[int]time.Time)

	if err != nil {
		log.Fatal("error when read tablesMeta file")
	}
	return res
}

func (c *Coordinator) Server() {
	lis, err := net.Listen("tcp", EccDcr.CLOUD_PORT)
	if err != nil {
		log.Fatal("failed to listen on port " + EccDcr.CLOUD_PORT)
	}
	server := grpc.NewServer()
	pb.RegisterCoordinatorServer(server, c)
	server.Serve(lis)
}

func (c *Coordinator) Close() {
	var wg sync.WaitGroup
	for i := 0; i < len(c.followers); i++ {
		wg.Add(1)
		go func(j int) {
			c.followers[j].CloseService(context.Background(), &emptypb.Empty{})
			wg.Done()
		}(i)
	}
	wg.Wait()
	for _, conn := range c.followersConn {
		conn.Close()
	}
	log.Println("close")
}

func (c *Coordinator) Register(ctx context.Context, args *pb.RegisterArgs) (*pb.RegisterReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply := &pb.RegisterReply{}
	reply.Id = -1
	p, ok := peer.FromContext(ctx)
	if !ok {
		return reply, nil
	}

	if c.workingTDCs == nil {
		addr := strings.Split(p.Addr.String(), ":")[0] + ":" + args.Address
		reply.Id = int32(len(c.followersAddr))
		c.followersAddr = append(c.followersAddr, addr)
	}

	return reply, nil
}

func (c *Coordinator) InitConnect() {
	for i, addr := range c.followersAddr {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal("can not connect worker address" + addr)
		} else {
			log.Println(fmt.Sprintf("worker %d address %s connect", i, addr))
		}
		c.followersConn = append(c.followersConn, conn)
		c.followers = append(c.followers, pb.NewWorkerClient(conn))
	}
}

func (c *Coordinator) CheckQuery(query string) (bool, string) {
	log.Println("start checking query...")
	query = strings.ReplaceAll(query, "\r\n", "\n")
	tdcs, ok, str := cc.Parse(query)
	if !ok {
		return false, str
	}

	for _, tdc := range tdcs {
		if tdc.Dc1 != nil {
			for _, p := range tdc.Dc1.Ps {
				if _ok, err := predicateInTable(p, c.tablesMeta); !_ok {
					return false, err
				}
			}
		}
		if tdc.Dc2 != nil {
			for _, p := range tdc.Dc2.Ps {
				if _ok, err := predicateInTable(p, c.tablesMeta); !_ok {
					return false, err
				}
			}
		}
	}

	table := str

	// check table in each worker
	var wg sync.WaitGroup
	cnt := int32(0)
	for i := 0; i < len(c.followers); i++ {
		wg.Add(1)
		go func(j int) {
			tableMeta := make(map[string]int32)
			for k, v := range c.tablesMeta[table] {
				tableMeta[k] = int32(v)
			}
			args := &pb.TableCheckArgs{Table: table, ColumnsType: tableMeta, Peers: c.followersAddr, Sep: c.tablesSep[table]}
			log.Println(args.ColumnsType)
			reply, _ := c.followers[j].TableCheck(context.Background(), args)
			if reply.Valid {
				atomic.AddInt32(&cnt, 1)
				log.Println("table check successfully in machine", j)
			} else {
				log.Fatal("failed to do TableCheck on machine", j)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	if cnt < int32(len(c.followers)) {
		return false, "table on some machines not consistent with table meta on coordinator"
	}

	go c.checkHeartbeat()

	c.workingTDCs = tdcs
	c.workingTable = table

	return true, ""
}

func operandInTable(tablesMeta map[string]map[string]core.ColumnType, operand cc.Operand) bool {
	if meta, ok := tablesMeta[operand.TableName]; ok {
		if _, ok = meta[operand.ColumnName]; ok {
			return true
		}
	}
	return false
}

func predicateInTable(p *cc.Predicate, tablesMeta map[string]map[string]core.ColumnType) (ok bool, err string) {
	if p.Operand1.Typ != cc.CONSTOPERAND && !operandInTable(tablesMeta, p.Operand1) {
		ok = false
		err = p.Operand1.ToString() + " not exist"
		if !ok {
			return
		}
	}
	if p.Operand2.Typ != cc.CONSTOPERAND && !operandInTable(tablesMeta, p.Operand2) {
		ok = false
		err = p.Operand2.ToString() + " not exist"
		if !ok {
			return
		}
	}

	return true, ""
}

func (c *Coordinator) Heartbeat(ctx context.Context, args *pb.HeartbeatArgs) (*emptypb.Empty, error) {
	id := int(args.Id)
	c.mu.Lock()
	defer c.mu.Unlock()

	c.heartbeat[id] = time.Now()

	return &emptypb.Empty{}, nil
}

func (c *Coordinator) checkHeartbeat() {
	for {
		time.Sleep(time.Duration(1) * time.Second)
		c.mu.Lock()
		for k, v := range c.heartbeat {
			if time.Now().Sub(v) > time.Duration(1) * time.Second {
				log.Println(fmt.Sprintf("worker %d fail to connect", k))
				os.Exit(1)
			}
		}
		c.mu.Unlock()
	}
}