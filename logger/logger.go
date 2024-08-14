package logger

type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

type ITransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)

	Run()
	Wait()
	Close() error
}

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}
