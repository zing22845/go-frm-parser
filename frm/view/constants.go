package view

type Algorithm int

const (
	Undefined Algorithm = iota
	TmpTable
	Merge
)

type SUIDType int

const (
	Invoker SUIDType = iota
	Definer
	Default
)

type CheckOption int

const (
	None CheckOption = iota
	Local
	Cascaded
)

func (a Algorithm) String() string {
	switch a {
	case Undefined:
		return "UNDEFINED"
	case TmpTable:
		return "TMPTABLE"
	case Merge:
		return "MERGE"
	default:
		return "UNDEFINED"
	}
}

func (s SUIDType) String() string {
	switch s {
	case Invoker:
		return "INVOKER"
	case Definer:
		return "DEFINER"
	case Default:
		return "DEFAULT"
	default:
		return "INVOKER"
	}
}

func (c CheckOption) String() string {
	switch c {
	case None:
		return "NONE"
	case Local:
		return "LOCAL"
	case Cascaded:
		return "CASCADED"
	default:
		return "NONE"
	}
}
