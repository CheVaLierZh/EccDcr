package cc

import (
	"EccDcr/myutil"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type TDCConjuncType int

const (
	PREVIOUS TDCConjuncType = iota
	SINCE                   = iota
	NONE                    = iota
)

// TemporalDC recursive definition not implemented
type TemporalDC struct {
	Dc1      *DenialConstraint // can be nil
	Conjunc  TDCConjuncType
	Dc2      *DenialConstraint
	FromDate *time.Time // can be nil
	ToDate   *time.Time
}

func (tdc *TemporalDC) GetType() TDCConjuncType {
	return tdc.Conjunc
}

func MakeTemporalDC(dc1, dc2 *DenialConstraint, conjunc TDCConjuncType, fromDate *time.Time, toDate *time.Time) *TemporalDC {
	return &TemporalDC{dc1, conjunc, dc2, fromDate, toDate}
}

// SinceInfer
//  dc2Acc accumulate cnt of conflict from minTimePoint to timePoints[i] - 1
//  dc1Acc accumulate cnt of conflict from maxTimePoint to timePoints[i]
func (tdc *TemporalDC) SinceInfer(timePoints []time.Time, dc1Acc []int32, dc2Acc []int32) time.Time { // timePoints >= tdc.fromDate && timePoints <= tdc.toDate
	if tdc.Conjunc != SINCE {
		panic("previous tdc can not use infer")
	}

	res := time.Time{}
	minCount := int32(1 << 31 - 1)
	for i := range timePoints {
		if dc1Acc[i] + dc2Acc[i] < minCount {
			res = timePoints[i]
			minCount = dc1Acc[i] + dc2Acc[i]
		}
	}

	return res
}

func (tdc *TemporalDC) ToDCs(tablesTemporalAttr map[string]string) []*DenialConstraint {
	res := make([]*DenialConstraint, 0)
	containing := make(map[string]string)
	for _, p := range tdc.Dc1.Ps {
		if p.Operand1.TableName != "" {
			containing[p.Operand1.TableName] = tablesTemporalAttr[p.Operand1.TableName]
		}
		if p.Operand2.TableName != "" {
			containing[p.Operand2.TableName] = tablesTemporalAttr[p.Operand2.TableName]
		}
	}
	switch tdc.Conjunc {
	case PREVIOUS:
		if tdc.FromDate != nil {
			for k, v := range containing {
				tdc.Dc1.Ps = append(tdc.Dc1.Ps, MakePredicate(">=", MakeOperand("", VAROPERAND, k, v), MakeOperand(tdc.FromDate.String(), CONSTOPERAND, "", "")))
			}
		}
		if tdc.ToDate != nil {
			for k, v := range containing {
				tdc.Dc1.Ps = append(tdc.Dc1.Ps, MakePredicate("<=", MakeOperand("", VAROPERAND, k, v), MakeOperand(tdc.FromDate.String(), CONSTOPERAND, "", "")))
			}
		}
		res = append(res, tdc.Dc1)
	case SINCE:
		res = append(res, tdc.Dc1)
		res = append(res, tdc.Dc2)
	case NONE:
		res = append(res, tdc.Dc1)
	}
	return res
}

// Parse
//      arguments: line: in the format of DECLARE s, t IN T, TDC[;|\n ...]
//                 TDC: dc | PREVIOUS<(d1,d2)> dc | dc1 SINCE<(d1,d2)> dc2
//                 dc: p1 and p2 and ...
//                 p in the format of s.A op t.B
//      return: if bool == true, string = table
//              else string = error message
func Parse(query string) ([]*TemporalDC, bool, string) {
	query = strings.TrimSpace(query)

	res := make([]*TemporalDC, 0)

	lines := strings.FieldsFunc(query, func(r rune) bool {
		return r == '\n' || r == ';'
	})

	declareIndex := strings.Index(lines[0], "DECLARE")
	if declareIndex != 0 {
		return nil, false, "lack DECLARE subexpr"
	}
	lastCommaIndex := strings.LastIndex(lines[0], ",")
	if lastCommaIndex == -1 {
		return nil, false, "syntax error in declare subexpr"
	}
	declareSubStr := strings.TrimSpace(lines[0][7:lastCommaIndex])
	varbs, table, ok := declareParse(declareSubStr)
	if !ok {
		return nil, false, "error in " + declareSubStr
	}
	if len(varbs) != 2 {
		return nil, false, "variables in DECLARE should be 2"
	}
	lines[0] = lines[0][lastCommaIndex+1:]

	for _, line := range lines {
		sinceIndex := strings.Index(line, "SINCE")
		previousIndex := strings.Index(line, "PREVIOUS")
		var date1 *time.Time
		var date2 *time.Time
		switch {
		case sinceIndex != -1:
			if previousIndex != -1 {
				return nil, false, "error in " + line
			}
			if line[sinceIndex+5] == '(' {
				rParenthese := strings.Index(line[sinceIndex:], ")")
				if rParenthese == -1 {
					return nil, false, "error in " + line
				} else {
					dateRangeStr := line[sinceIndex+6 : rParenthese]
					date1, date2, ok = dateRangeParse(dateRangeStr)
					if !ok {
						return nil, false, "error in " + line
					}
				}
			}

			splits1 := strings.Split(strings.TrimSpace(line[:sinceIndex]), " AND ")
			splits2 := strings.Split(strings.TrimSpace(line[sinceIndex+5:]), " AND ")

			dc1, ok := dcParse(splits1, varbs, table)
			if !ok {
				return nil, false, "error in " + line
			}
			dc2, ok := dcParse(splits2, varbs, table)
			if !ok {
				return nil, false, "error in " + line
			}

			res = append(res, MakeTemporalDC(dc1, dc2, SINCE, date1, date2))

		case previousIndex != -1:
			if sinceIndex != -1 {
				return nil, false, "error in " + line
			}
			if line[previousIndex+8] == '(' {
				rParenthese := strings.Index(line[sinceIndex:], ")")
				if rParenthese == -1 {
					return nil, false, "error in " + line
				} else {
					dateRangeStr := line[sinceIndex+9 : rParenthese]
					date1, date2, ok = dateRangeParse(dateRangeStr)
					if !ok {
						return nil, false, "error in " + line
					}
				}

				splits := strings.Split(strings.TrimSpace(line[rParenthese+1:]), " AND ")

				dc1, ok := dcParse(splits, varbs, table)
				if !ok {
					return nil, false, "error in " + line
				}

				res = append(res, MakeTemporalDC(dc1, nil, PREVIOUS, date1, date2))
			} else {
				return nil, false, "error in " + line
			}

		default:
			splits := strings.Split(strings.TrimSpace(line), "AND")
			dc1, ok := dcParse(splits, varbs, table)
			if !ok {
				return nil, false, "error in " + line
			}

			res = append(res, MakeTemporalDC(dc1, nil, NONE, nil, nil))
		}
	}

	return res, true, table
}

func dateRangeParse(str string) (*time.Time, *time.Time, bool) {
	var date1 *time.Time
	var date2 *time.Time
	splits := strings.Split(str, ",")
	if len(splits) != 2 {
		return nil, nil, false
	}
	split1 := strings.TrimSpace(splits[0])
	if split1 == "-" {
		date1 = nil
	} else if t, err := time.Parse(split1, split1); err == nil {
		date1 = &t
	} else {
		return nil, nil, false
	}
	split2 := strings.TrimSpace(splits[1])
	if split2 == "-" {
		date2 = nil
	} else if t, err := time.Parse(split2, split2); err == nil {
		date2 = &t
	} else {
		return nil, nil, false
	}

	return date1, date2, true
}

func dcParse(strs []string, varbs []string, table string) (*DenialConstraint, bool) {
	if len(strs) == 1 && strings.TrimSpace(strs[0]) == "true" {
		return nil, true
	}
	dc := MakeDenialConstraint()
	for _, s := range strs {
		p, ok := predicateParse(s, varbs, table)
		if !ok {
			return nil, false
		}
		dc.Ps = append(dc.Ps, p)
	}
	return dc, true
}

func declareParse(str string) (varbs []string, table string, ok bool) {
	r, _ := regexp.Compile("(\\w+)(, *\\w+)* *IN *(\\w+)")
	if !r.MatchString(str) {
		return nil, "",false
	}

	varbs = make([]string, 0)
	matches := r.FindStringSubmatch(str)

	table = matches[len(matches)-1]
	for i := 1; i < len(matches)-1; i++ {
		tmp := matches[i]
		if tmp[0] == ',' {
			tmp = tmp[1:]
		}
		for tmp[0] == ' ' {
			tmp = tmp[1:]
		}
		varbs = append(varbs, tmp)
	}

	return varbs, table,true
}

func predicateParse(str string, varbs []string, table string) (*Predicate, bool) {
	var res *Predicate
	var ok bool
	if strings.Index(str, "!=") != -1 {
		res, ok = typePredicateParse(str, "!=", varbs, table)
	} else if strings.Index(str, ">=") != -1 {
		res, ok = typePredicateParse(str, ">=", varbs, table)
	} else if strings.Index(str, "<=") != -1 {
		res, ok = typePredicateParse(str, "<=", varbs, table)
	} else if strings.Index(str, "=") != -1 {
		res, ok = typePredicateParse(str, "=", varbs, table)
	} else if strings.Index(str, ">") != -1 {
		res, ok = typePredicateParse(str, ">", varbs, table)
	} else if strings.Index(str, "<") != -1 {
		res, ok = typePredicateParse(str, "<", varbs, table)
	} else {
		res, ok = nil, false
	}

	return res, ok
}

func typePredicateParse(str string, op string, varbs []string, table string) (*Predicate, bool) {
	var res *Predicate
	idx := strings.Index(str, op)
	lhs := strings.TrimSpace(str[:idx])
	rhs := strings.TrimSpace(str[idx+len(op):])
	lhOperand, lidx, lOk := operandParse(lhs, varbs, table)
	rhOperand, ridx, rOk := operandParse(rhs, varbs, table)
	if !lOk || !rOk {
		return nil, false
	}
	if lidx < ridx {
		res = MakePredicate(op, lhOperand, rhOperand)
	} else {
		res = MakePredicate(op, rhOperand, lhOperand)
	}
	return res, true
}

func operandParse(str string, varbs []string, table string) (*Operand, int, bool) {
	var res *Operand
	idx := -1
	if strings.Index(str, ".") != -1 {
		if _, err := strconv.ParseFloat(str, 64); err == nil {
			res = MakeOperand(str, CONSTOPERAND, "", "")
			idx = 2
		} else {
			splits := strings.Split(str, ".")
			if idx = myutil.StringFind(varbs, splits[0]); idx >= 0 {
				res = MakeOperand("", VAROPERAND, table, splits[1])
			} else {
				return nil, -1, false
			}
		}
	} else {
		res = MakeOperand(str, CONSTOPERAND, "", "")
		idx = 2
	}
	return res, idx, true
}
