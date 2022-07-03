package core

import (
	"EccDcr/myutil"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

/*
	It efficiently represents a collection of row_id from one table on one machine with the same attribute value.
	It only used in the data of single machine.
*/
type Cluster struct {
	tuples *roaring.Bitmap // tuples == nil OR
}

func MakeCluster() *Cluster {
	c := Cluster{}
	c.tuples = roaring.New()
	return &c
}

func MakeClusterUnion() *Cluster {
	return &Cluster{tuples: nil}
}

func (c *Cluster) Add(x uint32) {
	c.tuples.Add(x)
}

func (c *Cluster) Or(other *Cluster) {
	if other.tuples == nil {
		c.tuples = nil
	} else if c.tuples != nil {
		c.tuples.Or(other.tuples)
	}
}

func (c *Cluster) And(other *Cluster) {
	if other.tuples == nil {
		return
	}
	if c.tuples == nil {
		c.tuples = roaring.New()
		c.tuples.Or(other.tuples)
	} else {
		c.tuples.And(other.tuples)
	}
}

func (c *Cluster) Remove(x uint32) {
	if c.tuples == nil {
		panic("can not remove on a default union cluster")
	}
	c.tuples.Remove(x)
}

func (c *Cluster) AndNot(x *Cluster) {
	if x.tuples == nil {
		c.tuples = roaring.New()
	} else if c.tuples == nil {
		c.tuples = roaring.New()
		c.tuples.Or(roaring.Flip(x.tuples, uint64(x.tuples.Minimum()), uint64(x.tuples.Maximum())))
	} else {
		c.tuples.AndNot(x.tuples)
	}
}

func (c *Cluster) Contains(x uint32) bool {
	if c.tuples == nil {
		return true
	} else {
		return c.tuples.Contains(x)
	}
}

func (c *Cluster) Size() int {
	if c.tuples == nil {
		panic("can not do size on a default cluster")
	}
	return int(c.tuples.GetCardinality())
}

func (c *Cluster) ToBytes() []byte {
	if c.tuples == nil {
		return []byte{}
	}
	res, _ := c.tuples.ToBytes()
	return res
}

func (c *Cluster) AddRange(a, b uint32) {
	if c.tuples != nil {
		c.tuples.AddRange(uint64(a), uint64(b))
	}
}

func (c *Cluster) FromBytes(data []byte) error {
	if len(data) == 0 {
		c.tuples = nil
		return nil
	}
	return c.tuples.UnmarshalBinary(data)
}

type IntIterable32 interface {
	HasNext() bool
	Next() uint32
}

func (c *Cluster) Iterator() IntIterable32 {
	if c.tuples == nil {
		panic("can not iterate on a default union cluster64")
	}
	return c.tuples.Iterator()
}

func (c *Cluster) ReverseIterator() IntIterable32 {
	if c.tuples == nil {
		panic("can not iterate on a default union cluster64")
	}
	return c.tuples.ReverseIterator()
}

func (c *Cluster) DefaultUnion() bool {
	return c.tuples == nil
}

/*
	It efficiently represents a collection of row_id(uint32) from different tables and different machines.
	Each row_id is encoded with table_id(8bit) and machine_id(8bit) to uint64 in the form of table_id|machine_id|row_id.
	It used in the cross-machine data.
*/

func Decode(x uint64) (int, uint32) {
	return int(x>>32), uint32(x & 0xffffffff)
}

func Encode(machineId int, x uint32) (y uint64) {
	y = uint64(x)
	y = y | (uint64(machineId) << 32)
	return y
}

type Cluster64 struct {
	tuples map[int]*roaring.Bitmap  // tuples == nil OR k, v != nil
}

func MakeCluster64Union() *Cluster64 {
	return &Cluster64{nil}
}

func (c *Cluster64)DeepCopy() *Cluster64 {
	res := &Cluster64{nil}
	if c.tuples != nil {
		res.tuples = make(map[int]*roaring.Bitmap)
		for k, v := range c.tuples {
			res.tuples[k] = roaring.New()
			res.tuples[k].Or(v)
		}
	}
	return res
}

func (c *Cluster64) Remove(x uint64) {
	if c.tuples == nil {
		panic("can not remove on a default union cluster64")
	}
	m, y := Decode(x)
	if _, ok := c.tuples[m]; ok {
		c.tuples[m].Remove(y)
	}
}

func (c *Cluster64) DefaultUnion() bool {
	return c.tuples == nil
}

func (c *Cluster64) AndNot(x *Cluster64) {

	if c.tuples == nil || x.tuples == nil {
		panic("can not do AndNot on a default union cluster64")
	}
	for k, v := range x.tuples {
		if _, ok := c.tuples[k]; ok {
			c.tuples[k].AndNot(v)
		}
	}
}

func (c *Cluster64) ToBytes() []byte {
	if c.tuples == nil {
		return []byte{}
	}
	tmp := make(map[int][]byte)
	for k, v := range c.tuples {
		tmp[k], _ = v.ToBytes()
	}
	res, _ := json.Marshal(tmp)
	return res
}

func (c *Cluster64) BatchAdd(key int, val *Cluster) {
	if val.tuples == nil {
		panic("batch add cluster can not be default union")
	} else if c.tuples == nil {
		c.tuples = make(map[int]*roaring.Bitmap)
		c.tuples[key] = roaring.New()
		c.tuples[key].Or(val.tuples)
	} else {
		if _, ok := c.tuples[key]; !ok {
			c.tuples[key] = roaring.New()
		}
		c.tuples[key].Or(val.tuples)
	}
}

func (c *Cluster64) Decompose() map[int]*Cluster {
	if c.tuples == nil {
		panic("can not do decompose on a default union cluster64")
	}
	res := make(map[int]*Cluster)
	for k, v := range c.tuples {
		res[k] = &Cluster{tuples: v}
	}

	return res
}

func (c *Cluster64) IsDefaultUnion() bool {
	return c.tuples == nil
}

func (c *Cluster64) FromBytes(data []byte) error {
	if len(data) == 0 {
		c.tuples = nil
		return nil
	}
	tmp := make(map[int][]byte)
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	c.tuples = make(map[int]*roaring.Bitmap)
	for k, v := range tmp {
		mp := roaring.New()
		err = mp.UnmarshalBinary(v)
		if err != nil {
			panic(err.Error())
		}
		c.tuples[k] = mp
	}
	return nil
}

func MakeCluster64() *Cluster64 {
	c := Cluster64{}
	c.tuples = make(map[int]*roaring.Bitmap)
	return &c
}

func (c *Cluster64) Add(x uint64) {
	if c.tuples != nil {
		m, y := Decode(x)
		if _, ok := c.tuples[m]; !ok {
			c.tuples[m] = roaring.New()
			c.tuples[m].Add(y)
		} else {
			c.tuples[m].Add(y)
		}
	}
}

func (c *Cluster64) Or(other *Cluster64) {
	if other.tuples == nil {
		c.tuples = nil
	} else if c.tuples != nil {
		for k, v := range other.tuples {
			if _, ok := c.tuples[k]; !ok {
				c.tuples[k] = roaring.New()
			}
			c.tuples[k].Or(v)
		}
	}
}

func (c *Cluster64) And(other *Cluster64) {
	if other.tuples == nil {
	} else if c.tuples == nil {
		c.tuples = make(map[int]*roaring.Bitmap)
		for k, v := range other.tuples {
			c.tuples[k] = roaring.New()
			c.tuples[k].Or(v)
		}
	} else {
		for k, v := range other.tuples {
			if bm, ok := c.tuples[k]; ok {
				bm.And(v)
			}
		}
		for k, _ := range c.tuples {
			if _, ok := other.tuples[k]; !ok {
				delete(c.tuples, k)
			}
		}
	}
}

func (c *Cluster64) Contains(x uint64) bool {
	if c.tuples == nil {
		return true
	}
	m, y := Decode(x)
	if _, ok := c.tuples[m]; !ok {
		return false
	}
	return c.tuples[m].Contains(y)
}

func (c *Cluster64) Size() uint64 {
	if c.tuples == nil {
		panic("can not do size on a default union cluster64")
	}
	res := uint64(0)
	for _, v := range c.tuples {
		res += v.GetCardinality()
	}
	return res
}

type IntIterable64 interface {
	HasNext() bool
	Next() uint64
}

type cluster64IntIterator struct {
	keys []int
	idx int
	iter IntIterable32
	c *Cluster64
}

func makecluster64IntIterator(c *Cluster64) *cluster64IntIterator {
	if c.tuples == nil {
		panic("can not iterate on a default union cluster64")
	}
	res := &cluster64IntIterator{}
	res.c = c
	res.keys = make([]int, len(c.tuples))
	i := 0
	for k := range c.tuples {
		res.keys[i] = k
		i++
	}
	res.idx = 0
	if res.idx >= len(res.c.tuples) {
		res.iter = nil
	} else {
		res.iter = res.c.tuples[res.keys[res.idx]].Iterator()
	}
	return res
}

func (iter *cluster64IntIterator) HasNext() bool {
	if iter.iter == nil {
		return false
	}
	if iter.iter.HasNext() {
		return true
	} else {
		for {
			iter.idx++
			if iter.idx >= len(iter.keys) {
				return false
			} else {
				iter.iter = iter.c.tuples[iter.keys[iter.idx]].Iterator()
				if iter.iter.HasNext() {
					return true
				}
			}
		}
	}
}

func (iter *cluster64IntIterator) Next() uint64 {
	return Encode(iter.keys[iter.idx], iter.iter.Next())
}

func (c *Cluster64) Iterator() IntIterable64 {
	return makecluster64IntIterator(c)
}

type PairIterable interface {
	HasNext() bool
	Next() (uint64, uint64)
}

type PairCluster interface {
	GetAdjacents(x uint64) *Cluster64
	Find(x, y uint64) bool
	Iterator() PairIterable
}

type AdjPairCluster struct {
	x   uint64
	adj *Cluster64
}

func MakeAdjPairCluster(x uint64, adj *Cluster64) *AdjPairCluster {
	return &AdjPairCluster{x, adj}
}

func (apc *AdjPairCluster) GetAdjacents(x uint64) *Cluster64 {
	if apc.x == x {
		return apc.adj
	}
	return nil
}

func (apc *AdjPairCluster) Find(x, y uint64) bool {
	if apc.x == x && apc.adj.Contains(y) {
		return true
	}
	if apc.x == y && apc.adj.Contains(x) {
		return true
	}
	return false
}

type AdjPairClusterIterator struct {
	x     uint64
	cIter IntIterable64
}

func MakeAdjPairClusterIterator(apc *AdjPairCluster) *AdjPairClusterIterator {
	return &AdjPairClusterIterator{x: apc.x, cIter: apc.adj.Iterator()}
}

func (iter *AdjPairClusterIterator) HasNext() bool {
	return iter.cIter.HasNext()
}

func (iter *AdjPairClusterIterator) Next() (uint64, uint64) {
	return iter.x, iter.cIter.Next()
}

/*
	It represents (uint64, uint64) collection which can be shown with cartesian product of the cluster with the same index.
	It should be used in the result of equal predicate and is singleton in one machine
	It should satisfy len(cArr1) == len(cArr2) and the size of any cluster included can not be zero
	It should be used in the cross-machine data.
*/
type EqPairCluster struct {
	cArr1 []*Cluster64
	cArr2 []*Cluster64
}

func MakeEqPairCluster() *EqPairCluster {
	res := &EqPairCluster{}
	return res
}

func (pc *EqPairCluster) AddPair(x, y *Cluster64) {
	pc.cArr1 = append(pc.cArr1, x)
	pc.cArr2 = append(pc.cArr2, y)
}

func (pc *EqPairCluster) GetAdjacents(x uint64) *Cluster64 {
	for i, c1 := range pc.cArr1 {
		if c1.Contains(x) {
			return pc.cArr2[i]
		}
	}
	return nil
}

func (pc *EqPairCluster) Find(x, y uint64) bool {
	for i, c := range pc.cArr1 {
		if c.Contains(x) {
			if pc.cArr2[i].Contains(y) {
				return true
			} else {
				return false
			}
		}
	}
	return false
}

type EqPairClusterIterator struct {
	idx      int
	c1Iter   IntIterable64
	c2Iter   IntIterable64
	pc       *EqPairCluster
	c1CurVal uint64
}

func MakeEqPairClusterIterator(pc *EqPairCluster) *EqPairClusterIterator {
	if len(pc.cArr1) == 0 {
		return nil
	}

	res := &EqPairClusterIterator{}
	res.idx = 0
	res.c1Iter = pc.cArr1[res.idx].Iterator()
	res.c2Iter = pc.cArr2[res.idx].Iterator()
	res.pc = pc
	res.c1CurVal = res.c1Iter.Next()
	return res
}

func (iter *EqPairClusterIterator) HasNext() bool {
	if !iter.c2Iter.HasNext() {
		iter.idx++
		if iter.idx >= len(iter.pc.cArr1) {
			return false
		} else {
			iter.c1Iter = iter.pc.cArr1[iter.idx].Iterator()
			iter.c1CurVal = iter.c1Iter.Next()
			iter.c2Iter = iter.pc.cArr2[iter.idx].Iterator()
			return true
		}
	}
	return true
}

func (iter *EqPairClusterIterator) Next() (uint64, uint64) {
	return iter.c1CurVal, iter.c2Iter.Next()
}

func (pc *EqPairCluster) Iterator() PairIterable {
	return MakeEqPairClusterIterator(pc)
}

type NeqPairCluster struct {
	u        *Cluster64
	cArr     []*Cluster64
	notInArr []*Cluster64
}

func MakeNeqPairCluster(u *Cluster64, cArr, notInArr []*Cluster64) *NeqPairCluster {
	res := &NeqPairCluster{}
	res.u = u
	res.cArr = cArr
	res.notInArr = notInArr
	return res
}

func (pc *NeqPairCluster) GetAdjacents(x uint64) *Cluster64 {
	for i, c := range pc.cArr {
		if c.Contains(x) {
			res := MakeCluster64()
			for iter := pc.u.Iterator(); iter.HasNext(); {
				p := iter.Next()
				if !pc.notInArr[i].Contains(p) {
					res.Add(p)
				}
			}
			return res
		}
	}
	return nil
}

func (pc *NeqPairCluster) Find(x, y uint64) bool {
	for i, c := range pc.cArr {
		if c.Contains(x) {
			if pc.u.Contains(y) && !pc.notInArr[i].Contains(y) {
				return true
			}
		}
	}
	return false
}

type NeqPairClusterIterator struct {
	idx     int
	cIter   IntIterable64
	uIter   IntIterable64
	pc      *NeqPairCluster
	cCurVal uint64
	uCurVal uint64
}

func MakeNeqPairClusterIterator(pc *NeqPairCluster) *NeqPairClusterIterator {
	res := &NeqPairClusterIterator{}
	res.idx = 0
	res.cIter = pc.cArr[res.idx].Iterator()
	res.uIter = pc.u.Iterator()
	res.pc = pc
	res.cCurVal = res.cIter.Next()
	res.uCurVal = uint64(0)
	return res
}

func (iter *NeqPairClusterIterator) HasNext() bool {
	for true {
		if iter.uIter.HasNext() {
			iter.uCurVal = iter.uIter.Next()
			if !iter.pc.notInArr[iter.idx].Contains(iter.uCurVal) {
				return true
			}
		} else {
			iter.idx++
			if iter.idx >= len(iter.pc.cArr) {
				break
			}
			iter.cIter = iter.pc.cArr[iter.idx].Iterator()
			iter.cCurVal = iter.cIter.Next()
			iter.uIter = iter.pc.u.Iterator()
		}
	}
	return false
}

func (iter *NeqPairClusterIterator) Next() (uint64, uint64) {
	return iter.cCurVal, iter.uCurVal
}

func (pc *NeqPairCluster) Iterator() PairIterable {
	return MakeNeqPairClusterIterator(pc)
}

/*
	It represents (uint64, uint64) collection which can be shown with delta appending like a1*b1, a2*(b1+b2),...
	It can be used in the result of inequal predicate(in the form of ascending order with <, <=)
*/
type IneqPairCluster struct {
	cArr []*Cluster64
	dArr []*Cluster64
}

func MakeIneqPairCluster(cArr, dArr []*Cluster64) *IneqPairCluster {
	res := &IneqPairCluster{}
	res.cArr = cArr
	res.dArr = dArr
	return res
}

func (pc *IneqPairCluster) GetAdjacents(x uint64) *Cluster64 {
	for i, c := range pc.cArr {
		if c.Contains(x) {
			res := MakeCluster64()
			for j := 0; j <= i; j++ {
				res.Or(pc.dArr[j])
			}
			return res
		}
	}
	return nil
}

func (pc *IneqPairCluster) Find(x, y uint64) bool {
	for i, c := range pc.cArr {
		if c.Contains(x) {
			for j := 0; j <= i; j++ {
				if pc.dArr[j].Contains(y) {
					return true
				}
			}
		}
	}
	return false
}

type IneqPairClusterIterator struct {
	idx     int
	cIter   IntIterable64
	dIter   IntIterable64
	dIdx    int
	pc      *IneqPairCluster
	cCurVal uint64
}

func MakeIneqPairClusterIterator(pc *IneqPairCluster) *IneqPairClusterIterator {
	res := &IneqPairClusterIterator{}
	res.idx = 0
	res.cIter = pc.cArr[res.idx].Iterator()
	res.dIdx = 0
	res.dIter = pc.dArr[res.dIdx].Iterator()
	res.pc = pc
	res.cCurVal = res.cIter.Next()
	return res
}

func (iter *IneqPairClusterIterator) HasNext() bool {
	for true {
		if iter.dIter.HasNext() {
			return true
		} else {
			iter.dIdx++
			if iter.dIdx > iter.idx {
				iter.idx++
				if iter.idx >= iter.dIdx {
					break
				}
				iter.cIter = iter.pc.cArr[iter.idx].Iterator()
			}
			iter.dIter = iter.pc.dArr[iter.dIdx].Iterator()
		}
	}
	return false
}

func (iter *IneqPairClusterIterator) Next() (uint64, uint64) {
	return iter.cCurVal, iter.dIter.Next()
}

func (pc *IneqPairCluster) Iterator() PairIterable {
	return MakeIneqPairClusterIterator(pc)
}

// PairClusterPersist a key value persistent store,
type PairClusterPersist struct {
	persistDir string
	keys map[int]bool
}

func MakePairClusterPersist(dirname string) *PairClusterPersist {
	dir := filepath.Join(myutil.ExecPath(), dirname)
	if ok, _ := myutil.PathExists(dir); !ok {
		err := os.Mkdir(dir, os.ModePerm)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
	res := &PairClusterPersist{persistDir: dir}
	res.keys = make(map[int]bool)
	return res
}

func (pcp *PairClusterPersist) Put(key int, val map[uint64]*Cluster64) error {
	fileName := filepath.Join(pcp.persistDir, fmt.Sprintf("inter-%v.pc", key))
	f, err := os.OpenFile(fileName, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.ModePerm)
	defer f.Close()

	if err != nil {
		return err
	}

	pcp.keys[key] = true

	for k, v := range val {
		var buf bytes.Buffer

		buf.Write([]byte(strconv.FormatInt(int64(k), 10)))
		buf.Write([]byte("\t\t"))
		buf.Write(v.ToBytes())
		buf.Write([]byte("\n\n"))

		n, err := f.Write(buf.Bytes())
		if err == nil && n < buf.Len() {
			return io.ErrShortWrite
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (pcp *PairClusterPersist) GetAllKeys() []int {
	res := make([]int, 0, len(pcp.keys))
	for k := range pcp.keys {
		res = append(res, k)
	}
	return res
}

func (pcp *PairClusterPersist) Get(key int) map[uint64]*Cluster64 {
	if !pcp.keys[key] {
		return nil
	}

	res := make(map[uint64]*Cluster64)

	fileName := filepath.Join(pcp.persistDir, fmt.Sprintf("inter-%v.pc", key))
	f, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(fileName + "not exist")
	}

	scanner := bufio.NewScanner(f)
	scanner.Split(func(data[] byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.Index(data, []byte{'\n', '\n'}); i >= 0 {
			return i + 2, data[0:i], nil
		}
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	})

	cnt := 0
	for scanner.Scan() {
		cnt++
		data := scanner.Bytes()
		i := bytes.Index(data, []byte{'\t', '\t'})
		k, _ := strconv.ParseInt(string(data[0:i]), 10, 64)
		v := MakeCluster64()
		v.FromBytes(data[i+2:])
		res[uint64(k)] = v
	}

	return res
}

func (pcp *PairClusterPersist) Close() error {
	return os.RemoveAll(pcp.persistDir)
}

