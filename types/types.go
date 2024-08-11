package types

import (
	"sync"
)

var Store = struct {
	sync.RWMutex
	M map[string]string
}{M: make(map[string]string)}
