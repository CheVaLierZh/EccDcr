package estimating

import (
	"EccDcr/myutil"
	"encoding/json"
	"errors"
	"math"
	"sort"
)

type NumericBin struct {
	Low  float64
	High float64
	Cnt  float64
	Ndv  float64
}

type EquidepthHistogram struct {
	Bins    []NumericBin
	Max     float64
	Min     float64
	Cnt     float64
	Ndv     float64
	MaxBins int
}

func MakeEquidepthHistogram(n int) *EquidepthHistogram {
	res := &EquidepthHistogram{make([]NumericBin, 0, n), -math.MaxFloat64, math.MaxFloat64, 0, 0, n}
	return res
}

func (h *EquidepthHistogram) Construct(data []float64) error {
	if len(data) < 2*h.MaxBins {
		return errors.New("not enough data to construct histogram")
	}

	sort.Slice(data, func(i, j int) bool {
		return data[i] < data[j]
	})
	h.Min = data[0]
	h.Max = data[len(data)-1]
	h.Cnt = float64(len(data))
	nPerBin := len(data) / h.MaxBins
	for i := 0; i < h.MaxBins; i++ {
		if i != h.MaxBins-1 {
			freq := make(map[float64]bool)
			for j := 0; j < nPerBin; j++ {
				freq[data[i*nPerBin+j]] = true
			}
			h.Ndv += float64(len(freq))
			b := NumericBin{data[i*nPerBin], data[(i+1)*nPerBin-1], float64(nPerBin), float64(len(freq))}
			h.Bins = append(h.Bins, b)
		} else {
			freq := make(map[float64]bool)
			for j := i * nPerBin; j < len(data); j++ {
				freq[data[j]] = true
			}
			h.Ndv += float64(len(freq))
			b := NumericBin{data[i*nPerBin], data[len(data)-1], float64(len(data) - i*nPerBin), float64(len(freq))}
			h.Bins = append(h.Bins, b)
		}
	}

	sort.Slice(h.Bins, func(i, j int) bool {
		return h.Bins[i].Low <= h.Bins[j].Low
	})

	return nil
}

func (h *EquidepthHistogram) Count() float64 {
	return h.Cnt
}

func (h *EquidepthHistogram) Cardinality() float64 {
	return h.Ndv
}

func (h *EquidepthHistogram) PredicateEstimate(typ EstimateType, other Histogram) float64 {
	o, _ := other.(*EquidepthHistogram)
	return numericPredicateSizeEstimate(h.Bins, o.Bins, h.Cnt, o.Cnt, typ)
}

func (h *EquidepthHistogram) RangeEstimate(typ EstimateType, c interface{}) float64 {
	constant, _ := c.(float64)
	return numericRangeSizeEstimate(h.Cnt, h.Bins, constant, typ) / h.Cnt
}

func (h *EquidepthHistogram) ToBytes() ([]byte, error) {
	return json.Marshal(h)
}

func (h *EquidepthHistogram) FromBytes(data []byte) error {
	return json.Unmarshal(data, h)
}

type MaxdiffHistogram struct {
	Bins    []NumericBin
	Max     float64
	Min     float64
	Cnt     float64
	Ndv     float64
	MaxBins int
}

func MakeMaxdiffHistogram(n int) *MaxdiffHistogram {
	res := &MaxdiffHistogram{make([]NumericBin, 0, n), math.MaxFloat64, -math.MaxFloat64, 0, 0, n}
	return res
}

func (h *MaxdiffHistogram) Construct(data []float64) error {
	if len(data) < 2*h.MaxBins {
		return errors.New("not enough data to construct histogram")
	}
	freq := make(map[float64]float64)
	for _, x := range data {
		freq[x]++
	}
	h.Cnt += float64(len(data))
	h.Ndv = float64(len(freq))

	diffVals := make([]float64, 0, len(freq))
	for k := range freq {
		diffVals = append(diffVals, k)
	}
	sort.Slice(diffVals, func(i, j int) bool {
		return diffVals[i] < diffVals[j]
	})
	h.Min = diffVals[0]
	h.Max = diffVals[len(diffVals)-1]
	length := h.Max - h.Min

	diffArea := make([][]float64, len(diffVals))
	for i := 0; i < len(diffArea)-1; i++ {
		diffArea[i] = make([]float64, 2)
		diffArea[i][0] = float64(i)
		diffArea[i][1] = freq[diffVals[i]] / h.Cnt * (diffVals[i+1] - diffVals[i]) / length
	}
	for i := 0; i < len(diffArea)-2; i++ {
		diffArea[i][1] = diffArea[i+1][1] - diffArea[i][1]
		if diffArea[i][1] < 0 {
			diffArea[i][1] = -diffArea[i][1]
		}
	}
	diffArea = diffArea[:len(diffArea)-2]
	sort.Slice(diffArea, func(i, j int) bool {
		return diffArea[i][1] > diffArea[j][1]
	})

	sz := h.MaxBins - 1
	if sz > len(diffArea) {
		sz = len(diffArea)
	}
	diffArea = diffArea[:sz]
	sort.Slice(diffArea, func(i, j int) bool {
		return diffArea[i][0] < diffArea[j][0]
	})

	j := 0
	for i := 0; i < len(diffArea); i++ {
		idx := int(diffArea[i][0])
		bin := NumericBin{}
		bin.Low = diffVals[j]
		bin.High = diffVals[idx]
		for ; j <= idx; j++ {
			bin.Cnt += freq[diffVals[j]]
			bin.Ndv++
		}
		h.Bins = append(h.Bins, bin)
	}
	bin := NumericBin{}
	bin.Low = diffVals[j]
	bin.High = diffVals[len(diffVals)-1]
	for ; j < len(diffVals); j++ {
		bin.Cnt += freq[diffVals[j]]
		bin.Ndv++
	}
	h.Bins = append(h.Bins, bin)

	sort.Slice(h.Bins, func(i, j int) bool {
		return h.Bins[i].Low <= h.Bins[j].Low
	})

	return nil
}

func (h *MaxdiffHistogram) Count() float64 {
	return h.Cnt
}

func (h *MaxdiffHistogram) Cardinality() float64 {
	return h.Ndv
}

func (h *MaxdiffHistogram) PredicateEstimate(typ EstimateType, other Histogram) float64 {
	o, _ := other.(*MaxdiffHistogram)
	return numericPredicateSizeEstimate(h.Bins, o.Bins, h.Cnt, o.Cnt, typ)
}

func (h *MaxdiffHistogram) RangeEstimate(typ EstimateType, c interface{}) float64 {
	constant, _ := c.(float64)
	return numericRangeSizeEstimate(h.Cnt, h.Bins, constant, typ) / h.Cnt
}

func (h *MaxdiffHistogram) ToBytes() ([]byte, error) {
	return json.Marshal(h)
}

func (h *MaxdiffHistogram) FromBytes(data []byte) error {
	return json.Unmarshal(data, h)
}

type OptimalHistogram struct {
	Bins    []NumericBin
	Max     float64
	Min     float64
	Cnt     float64
	Ndv     float64
	MaxBins int
}

func MakeOptimalHistogram(n int) *OptimalHistogram {
	return &OptimalHistogram{make([]NumericBin, 0, n), math.MaxFloat64, -math.MaxFloat64, 0, 0, n}
}

func (h *OptimalHistogram) Construct(data []float64) error {
	if len(data) < 2*h.MaxBins {
		return errors.New("not enough data to construct histogram")
	}

	tmp := make(map[float64]float64)
	for _, v := range data {
		tmp[v] += 1
	}

	count := make([][]float64, 0)
	for k, v := range tmp {
		count = append(count, []float64{k, v})
	}
	sort.Slice(count, func(i, j int) bool {
		return count[i][0] < count[j][0]
	})

	h.Min = count[0][0]
	h.Max = count[len(count)-1][0]
	h.Ndv = float64(len(count))
	for _, c := range count {
		h.Cnt += c[1]
	}

	pp := make([]float64, len(count)+1)
	p := make([]float64, len(count)+1)
	for i := 1; i <= len(count); i++ {
		pp[i] = pp[i-1] + count[i-1][1]*count[i-1][1]
		p[i] = p[i-1] + count[i-1][1]
	}

	nBins := myutil.MinInt(h.MaxBins, len(count))
	dp := make([][]float64, nBins+1)
	mark := make([][]int, nBins+1)
	for i := 0; i <= nBins; i++ {
		dp[i] = make([]float64, len(count)+1)
		mark[i] = make([]int, len(count)+1)
	}

	for i := 1; i <= nBins; i++ {
		for j := i; j <= len(count); j++ {
			if i == 1 {
				dp[i][j] = pp[j] - p[j]*p[j]/float64(j)
			} else if j == i {
				dp[i][j] = 0.0
				mark[i][j] = j-1
			} else {
				dp[i][j] = math.MaxFloat64
				for k := i - 1; k < j; k++ {
					curr := dp[i-1][k] + pp[j] - pp[k] - (p[j]-p[k])*(p[j]-p[k])/float64(j-k)
					if curr < dp[i][j] {
						dp[i][j] = curr
						mark[i][j] = k
					}
				}
			}
		}
	}

	binSplit := make([]int, 0)
	binSplit = append(binSplit, len(count)-1)
	for i, j := nBins, len(count); i != 1; i-- {
		binSplit = append(binSplit, mark[i][j] - 1)
		j = mark[i][j]
	}

	sort.Slice(binSplit, func(i, j int) bool {
		return binSplit[i] < binSplit[j]
	})

	for i, j := 0, 0; i < len(binSplit); i++ {
		c := 0.0
		Ndv := 0.0
		b := NumericBin{}
		b.Low = count[j][0]
		b.High = count[binSplit[i]][0]
		for ; j <= binSplit[i]; j++ {
			Ndv += 1
			c += count[j][1]
		}
		b.Cnt = c
		b.Ndv = Ndv
		if c != 0.0 {
			h.Bins = append(h.Bins, b)
		}
	}

	return nil
}

func (h *OptimalHistogram) Count() float64 {
	return h.Cnt
}

func (h *OptimalHistogram) Cardinality() float64 {
	return h.Ndv
}

func (h *OptimalHistogram) PredicateEstimate(typ EstimateType, other Histogram) float64 {
	o, _ := other.(*OptimalHistogram)
	return numericPredicateSizeEstimate(h.Bins, o.Bins, h.Cnt, o.Cnt, typ)
}

func (h *OptimalHistogram) RangeEstimate(typ EstimateType, c interface{}) float64 {
	constant, _ := c.(float64)
	return numericRangeSizeEstimate(h.Cnt, h.Bins, constant, typ)
}

func (h *OptimalHistogram) ToBytes() ([]byte, error) {
	return json.Marshal(h)
}

func (h *OptimalHistogram) FromBytes(data []byte) error {
	return json.Unmarshal(data, h)
}

func numericRangeSizeEstimate(cnt float64, bins []NumericBin, c float64, typ EstimateType) float64 {
	res := 0.0
	switch typ {
	case EQ:
		for _, bin := range bins {
			if bin.Low <= c && c <= bin.High {
				res += bin.Cnt / bin.Ndv
			}
		}
	case GEQ:
		for _, bin := range bins {
			if bin.High >= c {
				if bin.Ndv == 1.0 {
					res += bin.Cnt
				} else {
					for i := 1; i <= int(bin.Ndv); i++ {
						if bin.Low+float64(i-1)/(bin.Ndv-1)*(bin.High-bin.Low) >= c {
							res += bin.Cnt / bin.Ndv
						}
					}
				}
			}
		}
	case GT:
		for _, bin := range bins {
			if bin.High > c {
				if bin.Ndv == 1.0 {
					res += bin.Cnt
				} else {
					for i := 1; i <= int(bin.Ndv); i++ {
						if bin.Low+float64(i-1)/(bin.Ndv-1)*(bin.High-bin.Low) > c {
							res += bin.Cnt / bin.Ndv
						}
					}
				}
			}
		}
	case LT:
		return cnt - numericRangeSizeEstimate(cnt, bins, c, GEQ)
	case LEQ:
		return cnt - numericRangeSizeEstimate(cnt, bins, c, GT)
	case NEQ:
		return cnt - numericRangeSizeEstimate(cnt, bins, c, EQ)
	}

	return res
}


func binSplitTo(aBins, bBins []NumericBin) ([]NumericBin, []NumericBin) {
	resa := make([]NumericBin, 0)
	resb := make([]NumericBin, 0)

	bounds := make([][]float64, 0)
	for _, bin := range aBins {
		bounds = append(bounds, []float64{bin.Low, 0})
		bounds = append(bounds, []float64{bin.High, 1})
	}
	for _, bin := range bBins {
		bounds = append(bounds, []float64{bin.Low, 0})
		bounds = append(bounds, []float64{bin.High, 1})
	}
	sort.Slice(bounds, func(i, j int) bool {
		return bounds[i][0] < bounds[j][0] || (bounds[i][0] == bounds[j][0] && bounds[i][1] < bounds[j][1])
	})

	for i := 1; i < len(bounds); i++ {
		if !(bounds[i-1][1] == 1 && bounds[i][1] == 0) {
			resa = append(resa, NumericBin{Low: bounds[i-1][0], High: bounds[i][0]})
			resb = append(resb, NumericBin{Low: bounds[i-1][0], High: bounds[i][0]})
		}
	}

	j := 0
	accQ := 0.0
	for i := 0; i < len(resa) && j < len(aBins); i++ {
		//log.Println(resa[i], aBins[j], i, j)
		if resa[i].Low >= aBins[j].Low && resa[i].High <= aBins[j].High { // (x - low) / (high - low) * (cnt - 2)  + 1
			if aBins[j].Low == aBins[j].High {
				resa[i].Cnt = aBins[j].Cnt
				resa[i].Ndv = aBins[j].Ndv
				accQ = 1
			} else {
				resa[i].Ndv = (resa[i].High - resa[i].Low) / (aBins[j].High - aBins[j].Low) * (aBins[j].Ndv - 2)
				if resa[i].Low == aBins[j].Low {
					resa[i].Ndv += 1
				}
				if resa[i].High == aBins[j].High {
					resa[i].Ndv += 1
				}
				q := math.Min(resa[i].Ndv / aBins[j].Ndv, 1 -accQ)
				resa[i].Ndv = q * aBins[j].Ndv
				resa[i].Cnt = q * aBins[j].Cnt
				accQ += q
			}
			if accQ >= 1 {
				j++
				accQ = 0
			}
		} else if resa[i].High <= aBins[j].Low {
			continue
		} else if resa[i].Low >= aBins[j].High {
			j++
			accQ = 0
		}
	}

	j = 0
	accQ = 0
	for i := 0; i < len(resb) && j < len(bBins); i++ {
		if resb[i].Low >= bBins[j].Low && resb[i].High <= bBins[j].High {
			if bBins[j].Low == bBins[j].High {
				resb[i].Cnt = bBins[j].Cnt
				resb[i].Ndv = bBins[j].Cnt
				accQ = 1
			} else {
				resb[i].Ndv = (resb[i].High - resb[i].Low) / (bBins[j].High - bBins[j].Low) * (bBins[j].Ndv - 2)
				if resb[i].Low == bBins[j].Low {
					resb[i].Ndv += 1
				}
				if resb[i].High == bBins[j].High {
					resb[i].Ndv += 1
				}
				q := math.Min(resb[i].Ndv / bBins[j].Ndv, 1 -accQ)
				resb[i].Ndv = q * bBins[j].Ndv
				resb[i].Cnt = q * bBins[j].Cnt
				accQ += q
			}
			if accQ >= 1 {
				j++
				accQ = 0
			}
		} else if resb[i].High <= bBins[j].Low {
			continue
		} else if resb[i].Low >= bBins[j].High {
			j++
			accQ = 0
		}
	}

	return resa, resb
}



func numericPredicateSizeEstimate(aBins, bBins []NumericBin, aCnt, bCnt float64, typ EstimateType) float64 {
	res := 0.0
	switch typ {
	case NEQ:
		res = aCnt * bCnt - numericPredicateSizeEstimate(aBins, bBins, aCnt, bCnt, EQ)
	case EQ:
		res = 0.0
		splita, splitb := binSplitTo(aBins, bBins)
		for i := 0; i < len(splita); i++ {
			if splita[i].Ndv != 0.0 || splitb[i].Ndv != 0.0 {
				res += splita[i].Cnt * splitb[i].Cnt / math.Max(splita[i].Ndv, splitb[i].Ndv)
			}
		}
	case GEQ:
		acc := 0.0
		splita, splitb := binSplitTo(aBins, bBins)
		for i := 0; i < len(splita); i++ {
			if splita[i].Ndv != 0.0 {
				res += splita[i].Cnt * (splitb[i].Cnt*(splita[i].Ndv+1)/(2*splita[i].Ndv) + acc)
			}
			acc += splitb[i].Cnt
		}
	case GT:
		acc := 0.0
		splita, splitb := binSplitTo(aBins, bBins)
		for i := 0; i < len(splita); i++ {
			if splita[i].Ndv != 0.0 {
				res += splita[i].Cnt * (splitb[i].Cnt*(splita[i].Ndv-1)/(2*splita[i].Ndv) + acc)
			}
			acc += splitb[i].Cnt
		}
	case LT:
		return numericPredicateSizeEstimate(bBins, aBins, bCnt, aCnt, GT)
	case LEQ:
		return numericPredicateSizeEstimate(bBins, aBins, bCnt, aCnt, GEQ)
	}
	return res
}
