package estimating

import (
	"encoding/json"
	"errors"
	pq "github.com/jupp0r/go-priority-queue"
)

type StringBin struct {
	Val string
	Cnt float64
}

type EndbiasedHistogram struct {
	GreaterBins []StringBin
	LessBins    []StringBin
	MaxBins     int
	Cnt         float64
	Ndv         float64
	Min         string
	MaX         string
	restAvg float64
}

func MakeEndbiasedHistogram(n int) *EndbiasedHistogram {
	return &EndbiasedHistogram{make([]StringBin, 0), make([]StringBin, 0), n, 0.0, 0.0, "", "", 0.0}
}

type KeyValue struct {
	key string
	val float64
}

func (h *EndbiasedHistogram) Construct(data []string) error {
	if len(data) < 2*h.MaxBins {
		return errors.New("not enough data to construct histogram")
	}

	freq := make(map[string]float64)
	h.Min = data[0]
	h.MaX = data[0]
	for _, d := range data {
		freq[d]++
		if h.Min > d {
			h.Min = d
		}
		if h.MaX < d {
			h.MaX = d
		}
	}
	h.Cnt = float64(len(data))
	h.Ndv = float64(len(freq))

	nBins := h.MaxBins
	if len(freq) < nBins {
		nBins = len(freq)
	}

	greaterPq := pq.New()
	lessPq := pq.New()
	for k, v := range freq {
		if greaterPq.Len() < nBins/2 {
			greaterPq.Insert(StringBin{k, v}, -v)
		} else {
			tmp, _ := greaterPq.Pop()
			top, _ := tmp.(StringBin)
			if top.Cnt < v {
				greaterPq.Insert(StringBin{k, v}, -v)
			} else {
				greaterPq.Insert(top, -top.Cnt)
			}
		}
		if lessPq.Len() < nBins-nBins/2 {
			lessPq.Insert(StringBin{k, v}, v)
		} else {
			tmp, _ := lessPq.Pop()
			top, _ := tmp.(StringBin)
			if top.Cnt > v {
				lessPq.Insert(StringBin{k, v}, v)
			} else {
				lessPq.Insert(top, top.Cnt)
			}
		}
	}

	alreadyIn := make(map[string]bool)
	for {
		tmp, err := greaterPq.Pop()
		if err != nil {
			break
		}
		top, _ := tmp.(StringBin)
		h.GreaterBins = append(h.GreaterBins, top)
		alreadyIn[top.Val] = true
	}
	for {
		tmp, err := lessPq.Pop()
		if err != nil {
			break
		}
		top, _ := tmp.(StringBin)
		h.LessBins = append(h.LessBins, top)
		alreadyIn[top.Val] = true
	}

	restCnt := 0.0
	restNdv := 0.0
	for k, v := range freq {
		if !alreadyIn[k] {
			restCnt += v
			restNdv++
		}
	}

	h.restAvg = restCnt / restNdv

	return nil
}

func (h *EndbiasedHistogram) Count() float64 {
	return h.Cnt
}

func (h *EndbiasedHistogram) Cardinality() float64 {
	return h.Ndv
}

func (h *EndbiasedHistogram) PredicateEstimate(typ EstimateType, other Histogram) float64 {
	o, _ := other.(*EndbiasedHistogram)
	res := 0.0
	for _, bin := range h.GreaterBins {
		res += o.stringRangeSizeEstimate(typ, bin.Val)
	}
	for _, bin := range h.LessBins {
		res += o.stringRangeSizeEstimate(typ, bin.Val)
	}
	return res
}

func (h *EndbiasedHistogram) RangeEstimate(typ EstimateType, c interface{}) float64 {
	constant, _ := c.(string)
	return h.stringRangeSizeEstimate(typ, constant)
}

func (h *EndbiasedHistogram) ToBytes() ([]byte, error) {
	return json.Marshal(h)
}

func (h *EndbiasedHistogram) FromBytes(data []byte) error {
	return json.Unmarshal(data, h)
}

func (h *EndbiasedHistogram) stringRangeSizeEstimate(typ EstimateType, c string) float64 {
	res := 0.0
	switch typ {
	case EQ:
		if c >= h.Min && c <= h.MaX {
			find := false
			for _, bin := range h.GreaterBins {
				if c == bin.Val {
					res += bin.Cnt
					find = true
					break
				}
			}
			if !find {
				for _, bin := range h.LessBins {
					if c == bin.Val {
						res += bin.Cnt
						find = true
						break
					}
				}
			}
			if !find {
				res += h.restAvg
			}
		}
	default:
		panic("string type can not perform operator except EQ, NEQ")
	}

	return res
}
