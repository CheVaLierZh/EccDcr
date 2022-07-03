package cc

import (
	"fmt"
)

type OperandType int

const (
	VAROPERAND   OperandType = iota
	CONSTOPERAND             = iota
)

type Operand struct {
	Val        string // "" iff Typ == VAROPERAND
	Typ        OperandType
	TableName  string
	ColumnName string
}

func MakeOperand(val string, typ OperandType, tableName string, columnName string) *Operand {
	operand := Operand{Val: val, Typ: typ, TableName: tableName, ColumnName: columnName}
	return &operand
}

func (o *Operand) ToString() string {
	if o.Typ == VAROPERAND {
		return o.TableName + "." + o.ColumnName
	} else if o.Typ == CONSTOPERAND {
		return fmt.Sprint(o.Val)
	}
	return ""
}

type PredicateType int

const (
	EQPREDICATE  PredicateType = iota
	NEQPREDICATE               
	GTPREDICATE
	GEQPREDICATE
	LTPREDICATE
	LEQPREDICATE
)

type Predicate struct {
	Typ      PredicateType
	Operand1 Operand
	Operand2 Operand
}

func MakePredicate(op string, operand1, operand2 *Operand) *Predicate {
	res := &Predicate{Operand1: *operand1, Operand2: *operand2}
	switch op {
	case "=":
		res.Typ = EQPREDICATE
	case "!=":
		res.Typ = NEQPREDICATE
	case ">":
		res.Typ = GTPREDICATE
	case ">=":
		res.Typ = GEQPREDICATE
	case "<":
		res.Typ = LTPREDICATE
	case "<=":
		res.Typ = LEQPREDICATE
	}
	return res
}

func (p *Predicate) ToString() string {
	op := ""
	switch p.Typ {
	case EQPREDICATE:
		op = "="
	case NEQPREDICATE:
		op = "!="
	case GTPREDICATE:
		op = ">"
	case GEQPREDICATE:
		op = ">="
	case LTPREDICATE:
		op = "<"
	case LEQPREDICATE:
		op = "<="
	}
	return p.Operand1.ToString() + " " + op + " " + p.Operand2.ToString()
}

type DenialConstraint struct {
	Ps []*Predicate
}

func MakeDenialConstraint() *DenialConstraint {
	return &DenialConstraint{make([]*Predicate, 0)}
}

func (dc *DenialConstraint) ToString() string {
	res := "not("
	for i, p := range dc.Ps {
		if i != 0 {
			res += " and "
		}
		res += p.ToString()
	}
	res += ")"
	return res
}
