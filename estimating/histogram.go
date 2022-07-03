package estimating

type EstimateType int

const (
	EQ  EstimateType = iota
	NEQ
	GEQ
	GT
	LT
	LEQ
)

type Histogram interface {
	Count() float64
	Cardinality() float64
	PredicateEstimate(EstimateType, Histogram) float64
	RangeEstimate(EstimateType, interface{}) float64
	ToBytes() ([]byte, error)
	FromBytes([]byte) error
}

type NumericHistogram interface {
	Construct([]float64) error
	Histogram
}

type StringHistogram interface {
	Construct([]string) error
	Histogram
}

/*
func binSplitBy(x []Bin, y []Bin) []Bin {
	res := make([]Bin, 0)

	vals := make([]float64, 0)
	max := -math.MaxFloat64
	for _, b := range x {
		vals = append(vals, b.Low)
		vals = append(vals, b.High)
		if max < b.High {
			max = b.High
		}
	}

	for _, b := range y {
		vals = append(vals, b.Low)
		vals = append(vals, b.High)
	}

	sort.Slice(vals, func(i, j int) bool {
		return vals[i] < vals[j]
	})

	j := 0
	prev := vals[0]
	quantileTotal := 0.0
	for i := 1; i < len(vals); i++ {
		b := Bin{prev, vals[i], 0, 0}
		if b.Low >= x[j].Low && b.High <= x[j].High {
			quantile := (b.High - b.Low + 1) / (x[j].High - x[j].Low + 1)
			b.Cnt = x[j].Cnt * quantile
			b.Ndv = x[j].Ndv * quantile
			quantileTotal += quantile
			if math.Abs(1-quantileTotal) <= 1e-6 {
				j++
				quantileTotal = 0.0
			}
		}
		res = append(res, b)
		fmt.Println(b)
		prev = vals[i]
	}

	return res
}
*/
