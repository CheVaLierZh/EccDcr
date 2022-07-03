package core

import (
	"EccDcr/estimating"
	"EccDcr/myutil"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ColumnType int

const (
	NUMERICHISTOGRAMTYPE = "Optimal"
	STRINGHISTOGRAMTYPE = "Endbiased"
)

const (
	STRING  ColumnType = iota
	NUMERIC
	LONG
	TIME
)

func IntToColumnType(x int) ColumnType {
	switch {
	case x == int(STRING):
		return STRING
	case x == int(NUMERIC):
		return NUMERIC
	case x == int(LONG):
		return LONG
	case x == int(TIME):
		return TIME
	default:
		panic(strconv.Itoa(x) + "can not be converted to column type")
	}
}

func ColumnVal2String(val interface{}, typ ColumnType) string {
	switch typ {
	case LONG:
		return fmt.Sprint(val)
	case NUMERIC:
		return fmt.Sprint(val)
	case TIME:
		return val.(time.Time).String()
	case STRING:
		return val.(string)
	}
	return ""
}

func columnValTransform(val string, typ ColumnType, args ...string) interface{} {
	switch typ {
	case LONG:
		ret, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			ret = 0
		}
		return int32(ret)
	case NUMERIC:
		ret, err := strconv.ParseFloat(val, 64)
		if err != nil {
			ret = 0.0
		}
		return ret
	case TIME:
		ret, err := time.Parse(args[0], val)
		if err != nil {
			ret = time.Time{}
		}
		return ret
	case STRING:
		return val
	}
	return val
}

// TableCache A LRU Cache to speed up search by row_idx
type TableCache struct {
	cache      map[uint32]*DLinkedNode
	size       int
	capacity   int
	head, tail *DLinkedNode
	mu sync.Mutex
}

type DLinkedNode struct {
	key        uint32
	val        []string
	prev, next *DLinkedNode
}

func MakeDLinkedNode(key uint32, data []string) *DLinkedNode {
	return &DLinkedNode{key, data, nil, nil}
}

func MakeTableCahce(capacity int) *TableCache {
	res := &TableCache{capacity: capacity}
	res.cache = make(map[uint32]*DLinkedNode)
	res.head = MakeDLinkedNode(0, nil)
	res.tail = MakeDLinkedNode(0, nil)
	res.head.next = res.tail
	res.tail.prev = res.head
	return res
}

func (tc *TableCache) Get(key uint32) ([]string, bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	node, ok := tc.cache[key]
	if !ok {
		return nil, false
	}

	tc.moveToHead(node)
	return node.val, true
}

func (tc *TableCache) Put(key uint32, val []string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if _, ok := tc.cache[key]; !ok {
		node := MakeDLinkedNode(key, val)
		tc.cache[key] = node
		tc.addToHead(node)
		tc.size++
		if tc.size > tc.capacity {
			removed := tc.removeTail()
			delete(tc.cache, removed.key)
			tc.size--
		}
	} else {
		node := tc.cache[key]
		tc.moveToHead(node)
	}
}

func (tc *TableCache) addToHead(node *DLinkedNode) {
	node.prev = tc.head
	node.next = tc.head.next
	tc.head.next.prev = node
	tc.head.next = node
}

func (tc *TableCache) removeNode(node *DLinkedNode) {
	prev := node.prev
	next := node.next
	prev.next = next
	next.prev = prev
}

func (tc *TableCache) moveToHead(node *DLinkedNode) {
	tc.removeNode(node)
	tc.addToHead(node)
}

func (tc *TableCache) removeTail() *DLinkedNode {
	node := tc.tail.prev
	tc.removeNode(node)
	return node
}

func SliceSort(a []interface{}, typ ColumnType) {
	sort.Slice(a, func(i, j int) bool {
		switch typ {
		case LONG:
			return a[i].(int32) < a[j].(int32)
		case NUMERIC:
			return a[i].(float64) < a[j].(float64)
		case TIME:
			return a[i].(time.Time).Before(a[j].(time.Time))
		case STRING:
			return a[i].(string) < a[j].(string)
		}
		return true
	})
}

type Table interface {
	Iterator() TableIterable
	Size() uint32
	GetTableName() string
	GetColumnsType() map[string]ColumnType
	GetColumnIndex(string) int
	GetColumnsName() []string
	ColumnValTransform(string, string) interface{}
	GetColumnType(col string) ColumnType
}


type TableIterable interface {
	Next() (uint32, []string, bool)
	NextFind(rowId uint32) []string
}

type TbTable struct {
	tableName   string
	colName2Typ map[string]ColumnType
	colsName    []string
	file        string
	mu sync.Mutex
	timeLayout string
	rows *Cluster
	sep string
}

func(tt *TbTable) GetColumnsName() []string {
	return tt.colsName
}

func (tt *TbTable) ColumnValTransform(val, col string) interface{} {
	return columnValTransform(val, tt.colName2Typ[col], tt.timeLayout)
}

func (tt *TbTable) GetColumnType(col string) ColumnType {
	return tt.colName2Typ[col]
}

func (tt *TbTable) GetColumnIndex(col string) int {
	for i := 0; i < len(tt.colsName); i++ {
		if tt.colsName[i] == col {
			return i
		}
	}

	return -1
}

func (tt *TbTable) BuildClusterIndex(col string) map[interface{}]*Cluster {
	colIdx := tt.GetColumnIndex(col)
	res := make(map[interface{}]*Cluster)
	iter := tt.Iterator()
	for {
		id, row, ok := iter.Next()
		if !ok {
			break
		}
		val := tt.ColumnValTransform(row[colIdx], col)
		if _, ok = res[val]; !ok {
			res[val] = MakeCluster()
		}
		res[val].Add(id)
	}
	return res
}

func (tt *TbTable) Append(data ...[]string) error {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	f, _ := os.OpenFile(tt.file, os.O_WRONLY | os.O_APPEND, os.ModePerm)
	defer f.Close()
	writer := csv.NewWriter(f)
	writer.Comma = rune(tt.sep[0])
	for _, d := range data {
		rowId, _ := strconv.ParseUint(d[len(d) - 1], 10, 32)
		tt.rows.Add(uint32(rowId))
		err := writer.Write(d)
		if err != nil {
			return err
		}
	}
	writer.Flush()

	return nil
}

func (tt *TbTable) GetTableName() string {
	return tt.tableName
}

type TbTableIterator struct {
	reader *csv.Reader
	tt *TbTable
	currRowId uint32
}

func MakeTbTableIterator(tt *TbTable) *TbTableIterator {
	f, _ := os.OpenFile(tt.file, os.O_RDONLY, os.ModePerm)
	reader := csv.NewReader(f)
	reader.Comma = rune(tt.sep[0])
	return &TbTableIterator{reader: reader, tt: tt}
}

func (iter *TbTableIterator) Next() (uint32, []string, bool) {
	row, err := iter.reader.Read()
	if err == io.EOF {
		return 0, nil, false
	}

	rowId, _ := strconv.ParseUint(row[len(row)-1], 10, 32)
	iter.currRowId = uint32(rowId)
	return uint32(rowId), row[0:len(row)-1], true
}

func (iter *TbTableIterator) NextFind(rowId uint32) []string {
	if rowId <= iter.currRowId {
		return nil
	}

	if !iter.tt.rows.Contains(rowId) {
		return nil
	}

	for {
		r, row, ok := iter.Next()
		if !ok {
			return nil
		}
		if r == rowId {
			return row
		}
	}
}

func (tt *TbTable) Iterator() TableIterable {
	return MakeTbTableIterator(tt)
}

func (tt *TbTable) Size() uint32 {
	return uint32(tt.rows.Size())
}

func (tt *TbTable) GetColumnsType() map[string]ColumnType {
	return tt.colName2Typ
}

type CsvTable struct {
	tableName        string
	colName2Typ      map[string]ColumnType
	colsName         []string
	file             string
	columnStatistics map[string]estimating.Histogram
	timeLayout string
	mu sync.Mutex
	rows *Cluster
	sep string
	tempoCol string
}

func (ct *CsvTable) GetTimeLayout() string {
	return ct.timeLayout
}

func (ct *CsvTable) GetColumnsName() []string {
	return ct.colsName
}

func (ct *CsvTable) GetTemporalColumnName() string {
	for k, v := range ct.colName2Typ {
		if v == TIME {
			return k
		}
	}
	return ""
}

const (
	SAMPLINGSCALE     float64 = 0.1
	MAXSAMPLINGSIZE   int     = 5000
	HISTOGRAMBINSCALE float64 = 0.1
	MAXBINSIZE        int     = 100
)

const (
	MAXBUFFERSIZE = 16 * 1024 * 1024
)

func (ct *CsvTable) GetColumnIndex(col string) int {
	for i := 0; i < len(ct.colsName); i++ {
		if ct.colsName[i] == col {
			return i
		}
	}

	return -1
}

func (ct *CsvTable) BuildClusterIndex(col string) map[interface{}]*Cluster {
	colIdx := ct.GetColumnIndex(col)
	res := make(map[interface{}]*Cluster)
	iter := ct.Iterator()
	for {
		id, row, ok := iter.Next()
		if !ok {
			break
		}
		val := ct.ColumnValTransform(row[colIdx], col)
		if _, ok = res[val]; !ok {
			res[val] = MakeCluster()
		}
		res[val].Add(id)
	}
	return res
}

func MakeCsvTable(colName2Typ map[string]ColumnType, tableName string, file string, sep string) (*CsvTable, error) {
	res := &CsvTable{tableName: tableName, colName2Typ: colName2Typ, file: file, sep: sep}
	res.rows = MakeCluster()

	timeAttr := ""
	for k, v := range colName2Typ {
		if v == TIME {
			if timeAttr == "" {
				timeAttr = k
			} else {
				return nil, errors.New("table contains more than one temporal attribute")
			}
		}
	}
	res.tempoCol = timeAttr

	f, err := os.OpenFile(file, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.Comma = rune(sep[0])
	res.colsName, err = reader.Read()
	if err != nil {
		log.Println("error while reading header")
	}

	for _, col := range res.colsName {
		if _, ok := res.colName2Typ[col]; !ok {
			return nil, errors.New("table file doest not contain column " + col)
		}
	}

	if timeAttr != "" {
		firstRow, _ := reader.Read()
		timeAttrVal := firstRow[res.GetColumnIndex(timeAttr)]
		_, res.timeLayout, err = myutil.ParseTimeInMySQLFormat(timeAttrVal)
		if err != nil {
			return nil, err
		}
	}


	size := uint32(1)
	for  {
		_, err = reader.Read()
		if err == io.EOF {
			break
		}
		size++
	}
	res.rows.AddRange(1, size + 1)

	res.columnStatistics = make(map[string]estimating.Histogram)

	samplingSize := int(float64(res.Size()) * SAMPLINGSCALE)
	if samplingSize > MAXSAMPLINGSIZE {
		samplingSize = MAXSAMPLINGSIZE
	}

	samples := res.reservoirSampling(samplingSize)

	nBins := int(float64(samplingSize) * HISTOGRAMBINSCALE)
	if nBins > MAXBINSIZE {
		nBins = MAXBINSIZE
	}

	for i, col := range res.colsName {
		switch res.colName2Typ[col] {
		case STRING:
			var hist estimating.StringHistogram
			switch STRINGHISTOGRAMTYPE {
			case "Endbiased":
				hist = estimating.MakeEndbiasedHistogram(nBins)
			default:
				return nil, errors.New("no histogram type " + STRINGHISTOGRAMTYPE)
			}
			data := make([]string, len(samples))
			for j, s := range samples {
				data[j] = s[i]
			}
			hist.Construct(data)
			res.columnStatistics[col] = hist
		case TIME:
			var hist estimating.NumericHistogram
			switch NUMERICHISTOGRAMTYPE {
			case "Maxdiff":
				hist = estimating.MakeMaxdiffHistogram(nBins)
			case "Equidepth":
				hist = estimating.MakeEquidepthHistogram(nBins)
			case "Optimal":
				hist = estimating.MakeOptimalHistogram(nBins)
			default:
				return nil, errors.New("no histogram type " + STRINGHISTOGRAMTYPE)
			}
			data := make([]float64, len(samples))
			for j, s := range samples {
				val := res.ColumnValTransform(s[i], col).(time.Time)
				data[j] = float64(val.Unix())
			}
			hist.Construct(data)
			res.columnStatistics[col] = hist
		case LONG:
			var hist estimating.NumericHistogram
			switch NUMERICHISTOGRAMTYPE {
			case "Maxdiff":
				hist = estimating.MakeMaxdiffHistogram(nBins)
			case "Equidepth":
				hist = estimating.MakeEquidepthHistogram(nBins)
			case "Optimal":
				hist = estimating.MakeOptimalHistogram(nBins)
			default:
				return nil, errors.New("no histogram type " + STRINGHISTOGRAMTYPE)
			}
			data := make([]float64, len(samples))
			for j, s := range samples {
				val := res.ColumnValTransform(s[i], col).(int32)
				data[j] = float64(val)
			}
			hist.Construct(data)
			res.columnStatistics[col] = hist
		case NUMERIC:
			var hist estimating.NumericHistogram
			switch NUMERICHISTOGRAMTYPE {
			case "Maxdiff":
				hist = estimating.MakeMaxdiffHistogram(nBins)
			case "Equidepth":
				hist = estimating.MakeEquidepthHistogram(nBins)
			case "Optimal":
				hist = estimating.MakeOptimalHistogram(nBins)
			default:
				return nil, errors.New("no histogram type " + STRINGHISTOGRAMTYPE)
			}
			data := make([]float64, len(samples))
			for j, s := range samples {
				val := res.ColumnValTransform(s[i], col).(float64)
				data[j] = val
			}
			hist.Construct(data)
			res.columnStatistics[col] = hist
		}
	}

	return res, nil
}

func (ct *CsvTable) reservoirSampling(n int) [][]string {
	samples := make([][]string, 0, n)
	iter := ct.Iterator()
	cnt := 0
	for {
		_, row, ok := iter.Next()
		if !ok {
			break
		}

		cnt++

		if len(samples) < n {
			samples = append(samples, row)
		} else {
			r := rand.Intn(cnt)
			if r < n {
				samples[r] = row
			}
		}
	}

	return samples
}

func (ct *CsvTable) GetColumnsType() map[string]ColumnType {
	return ct.colName2Typ
}

func (ct *CsvTable) GetColumnType(col string) ColumnType {
	return ct.colName2Typ[col]
}

func (ct *CsvTable) GetTableName() string {
	return ct.tableName
}

func (ct *CsvTable) GetColumnHistogram(column string) estimating.Histogram {
	return ct.columnStatistics[column]
}

func (ct *CsvTable)MakeTmpTable(file string) *TbTable {
	res := &TbTable{tableName: ct.tableName, colName2Typ: ct.colName2Typ, colsName: ct.colsName, file: file, sep: ct.sep}
	res.timeLayout = ct.timeLayout
	res.rows = MakeCluster()
	f, _ := os.OpenFile(file, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.ModePerm)
	f.Close()
	return res
}

func (ct *CsvTable) GetSep() string {
	return ct.sep
}

func (ct *CsvTable) DoRepair(c *Cluster) error {
	repairDir := filepath.Join(filepath.Dir(filepath.Dir(ct.file)), "repair")
	if ok, _ := myutil.PathExists(repairDir); !ok {
		err := os.Mkdir(repairDir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	fileName := filepath.Join(repairDir, "repair.csv")
	f, err := os.OpenFile(fileName, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.ModePerm)
	defer f.Close()

	if err != nil {
		return err
	}

	f.Write([]byte(strings.Join(ct.colsName, ct.sep) + "\n"))

	tIter := ct.Iterator()
	for {
		r, line, ok := tIter.Next()
		if !ok {
			break
		}
		if !c.Contains(r) {
			f.Write([]byte(strings.Join(line, ct.sep) + "\n"))
		}
	}

	return nil
}

func (ct *CsvTable) Size() uint32 {
	return uint32(ct.rows.Size())
}

func (ct *CsvTable) ColumnValTransform(val, col string) interface{} {
	return columnValTransform(val, ct.colName2Typ[col], ct.timeLayout)
}

func (ct *CsvTable) SetAliveRows(c *Cluster) {
	ct.rows = c
}

func (ct *CsvTable) GetAliveRows() *Cluster {
	return ct.rows
}

func (ct *CsvTable) GetColumnVals(c *Cluster, col string) []interface{} {
	res := make([]interface{}, 0)
	tIter := ct.Iterator()
	idx := ct.GetColumnIndex(col)
	for iter := c.Iterator(); iter.HasNext(); {
		r := iter.Next()
		val := tIter.NextFind(r)[idx]
		res = append(res, ct.ColumnValTransform(val, col))
	}
	return res
}

type CsvTableIterator struct {
	reader *csv.Reader
	currRowId uint32
	ct *CsvTable
}

func MakeCsvTableIterator(ct *CsvTable) *CsvTableIterator {
	f, _ := os.OpenFile(ct.file, os.O_RDONLY, os.ModePerm)
	reader := csv.NewReader(f)
	reader.Comma = rune(ct.sep[0])
	reader.Read()
	return &CsvTableIterator{reader: reader, ct: ct, currRowId: 0}
}

func (iter *CsvTableIterator) Next() (uint32, []string, bool) {
	for {
		row, err := iter.reader.Read()
		if err == io.EOF {
			break
		}
		iter.currRowId++
		if iter.ct.rows.Contains(iter.currRowId) {
			return iter.currRowId, row, true
		}
	}
	return 0, nil, false
}

func (iter *CsvTableIterator) NextFind(rowId uint32) []string {
	if iter.currRowId >= rowId {
		return nil
	}

	if !iter.ct.rows.Contains(rowId) {
		return nil
	}

	for {
		row, err := iter.reader.Read()
		if err == io.EOF {
			return nil
		}
		iter.currRowId++
		if iter.currRowId == rowId {
			return row
		}
	}
}

func (ct *CsvTable) Iterator() TableIterable {
	return MakeCsvTableIterator(ct)
}

func Eval(op string, v1 interface{}, v2 interface{}, typ ColumnType) bool {
	switch op {
	case "=":
		switch typ {
		case LONG:
			v1Conv, _ := v1.(int32)
			v2Conv, _ := v2.(int32)
			return v1Conv == v2Conv
		case NUMERIC:
			v1Conv, _ := v1.(float64)
			v2Conv, _ := v2.(float64)
			return v1Conv == v2Conv
		case STRING:
			v1Conv, _ := v1.(string)
			v2Conv, _ := v2.(string)
			return v1Conv == v2Conv
		case TIME:
			v1Conv, _ := v1.(time.Time)
			v2Conv, _ := v2.(time.Time)
			return v1Conv.Equal(v2Conv)
		}
	case "!=":
		return !Eval("=", v1, v2, typ)
	case ">=":
		switch typ {
		case LONG:
			v1Conv, _ := v1.(int32)
			v2Conv, _ := v2.(int32)
			return v1Conv >= v2Conv
		case NUMERIC:
			v1Conv, _ := v1.(float64)
			v2Conv, _ := v2.(float64)
			return v1Conv >= v2Conv
		case STRING:
			v1Conv, _ := v1.(string)
			v2Conv, _ := v2.(string)
			return v1Conv >= v2Conv
		case TIME:
			v1Conv, _ := v1.(time.Time)
			v2Conv, _ := v2.(time.Time)
			return v1Conv.Equal(v2Conv) || v1Conv.After(v2Conv)
		}
	case ">":
		switch typ {
		case LONG:
			v1Conv, _ := v1.(int32)
			v2Conv, _ := v2.(int32)
			return v1Conv > v2Conv
		case NUMERIC:
			v1Conv, _ := v1.(float64)
			v2Conv, _ := v2.(float64)
			return v1Conv > v2Conv
		case STRING:
			v1Conv, _ := v1.(string)
			v2Conv, _ := v2.(string)
			return v1Conv > v2Conv
		case TIME:
			v1Conv, _ := v1.(time.Time)
			v2Conv, _ := v2.(time.Time)
			return v1Conv.After(v2Conv)
		}
	case "<":
		return !Eval(">=", v1, v2, typ)
	case "<=":
		return !Eval(">", v1, v2, typ)
	}
	return false
}
