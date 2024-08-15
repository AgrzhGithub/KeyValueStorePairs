package types

import (
	"sync"
	"value/logger"
)

var Store = struct {
	sync.RWMutex
	M        map[string]string
	transact logger.TransactionLogger
}{M: make(map[string]string)}
