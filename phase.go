package EccDcr

import (
	"EccDcr/cc"
	"encoding/json"
	"sort"
)

type TreeNode struct {
	p        *cc.Predicate
	sel      float64
	cnt      float64
	owners   []int
	children []*TreeNode
	end      int
}

func MakeTreeNode(p *cc.Predicate, sel, cnt float64, owners []int) *TreeNode {
	return &TreeNode{p, sel, cnt, owners, nil, -1}
}

func MakePredicateTree(nodes []*TreeNode, n int) *TreeNode {
	root := MakeTreeNode(nil, 0.0, 0.0, nil)

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].cnt/nodes[i].sel > nodes[j].cnt/nodes[j].sel
	})

	nodeDeepest := make(map[int]*TreeNode)
	for i := 0; i < n; i++ {
		nodeDeepest[i] = root
	}
	for _, tn := range nodes {
		for _, i := range tn.owners {
			parent := nodeDeepest[i]
			parent.children = append(parent.children, tn)
			nodeDeepest[i] = tn
		}
	}

	for k, v := range nodeDeepest {
		v.end = k
	}

	return root
}

type Phase struct {
	ps       []*cc.Predicate
	next     []*Phase
	end      int
	accSel float64
}

func (t *TreeNode) BuildPhases(nMaxNodes int) *Phase {
	root := &Phase{nil, make([]*Phase, 0), -1, 1.0}
	for _, tn := range t.children {
		root.next = append(root.next, dfsBuildPhase(tn, nMaxNodes, root.accSel))
	}
	return root
}

func dfsBuildPhase(tn *TreeNode, nMaxNodes int, accSel float64) *Phase {
	res := &Phase{make([]*cc.Predicate, 0), make([]*Phase, 0), -1, accSel}
	for len(res.ps) < nMaxNodes {
		res.ps = append(res.ps, tn.p)
		res.accSel *= tn.sel
		res.end = tn.end
		if len(tn.children) == 0 {
			return res
		} else if len(tn.children) == 1 {
			tn = tn.children[0]
		} else {
			for _, child := range tn.children {
				res.next = append(res.next, dfsBuildPhase(child, nMaxNodes, res.accSel))
			}
			return res
		}
	}

	res.next = append(res.next, dfsBuildPhase(tn, nMaxNodes, res.accSel))

	return res
}

type PhaseIntPair struct {
	key *Phase
	val int
}

type PhaseProgess struct {
	root     *Phase
	size     int
	st       []PhaseIntPair
	nextId   int
}

func MakePhaseProgress(root *Phase) *PhaseProgess {
	res := &PhaseProgess{root: root}
	res.size = dfsCountTree(root)
	res.nextId = 0
	res.st = make([]PhaseIntPair, 0)
	res.st = append(res.st, PhaseIntPair{root, res.nextId})
	return res
}

func (pp *PhaseProgess) HasNext() bool {
	if len(pp.st) == 0 {
		return false
	}
	curr := pp.st[len(pp.st) - 1]
	return len(curr.key.next) != 0
}

func (pp *PhaseProgess) Next() (int, []LogicPlan, float64) {
	if len(pp.st) == 0 {
		return -1, nil, 0.0
	}
	prev := pp.st[len(pp.st)-1]
	pp.st = pp.st[:len(pp.st)-1]
	res := make([]LogicPlan, 0)
	for _, child := range prev.key.next {
		pp.nextId++
		res = append(res, *MakeLogicPlan(pp.nextId, child))
		pp.st = append(pp.st, PhaseIntPair{child, pp.nextId})
	}

	if len(res) == 0 {
		return prev.val, nil, prev.key.accSel
	}

	return prev.val, res, prev.key.accSel
}

func dfsCountTree(root *Phase) int {
	if root == nil {
		return 0
	}

	res := 0
	if len(root.ps) != 0 {
		res++
	}
	for _, child := range root.next {
		res += dfsCountTree(child)
	}
	return res
}

type LogicPlan struct {
	Ps          []cc.Predicate
	Id          int
	End         int
}

func MakeLogicPlan(id int, p *Phase) *LogicPlan {
	res := &LogicPlan{make([]cc.Predicate, 0), id, p.end}
	for _, v := range p.ps {
		res.Ps = append(res.Ps, *v)
	}
	return res
}

func (lp *LogicPlan) ToBytes() ([]byte, error) {
	return json.Marshal(lp)
}

func (lp *LogicPlan) FromBytes(data []byte) error {
	return json.Unmarshal(data, lp)
}