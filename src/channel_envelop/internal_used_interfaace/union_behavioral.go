package internal_used_interfaace

type UnionBehavioral interface {
	Notify(data ...any)
	Channel() <-chan any
	IsBroken() bool
}
