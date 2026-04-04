package main_interface

type ElectionResult interface {
	Accept() bool
	Why() error
	SetAccept(accept bool)
	SetWhy(why error)
}
