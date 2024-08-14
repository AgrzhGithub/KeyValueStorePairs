package logger

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

type TransactionLogger struct {
	events       chan<- Event
	errors       <-chan error
	lastSequence uint64
	File         *os.File
	wg           *sync.WaitGroup
}

func (l *TransactionLogger) WritePut(key, value string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}
func (l *TransactionLogger) WriteDelete(key string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventDelete, Key: key}
}
func (l *TransactionLogger) Err() <-chan error {
	return l.errors
}

func NewFileTransactionLogger(filename string) (*TransactionLogger, error) {
	var err error
	var l TransactionLogger = TransactionLogger{wg: &sync.WaitGroup{}}
	l.File, err = os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open logger log file: %w", err)
	}
	return &l, nil
}

func (l *TransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		for e := range events {
			l.lastSequence++

			_, err := fmt.Fprintf(l.File, "%d\t%d\t%s\t%s\n",
				l.lastSequence, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- fmt.Errorf("cannot write to log file: %w", err)
			}
			l.wg.Done()
		}

	}()
}

func (l *TransactionLogger) Wait() {
	l.wg.Wait()
}

func (l *TransactionLogger) Close() error {
	l.Wait()
	if l.events != nil {
		close(l.events)
	}
	return l.File.Close()
}

func (l *TransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.File)
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		var e Event

		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			fmt.Sscanf(line, "%d\t%d\t%s\t%s",
				&e.Sequence, &e.EventType, &e.Key, &e.Value)

			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transcation number out of sequence")
				return
			}
			l.lastSequence = e.Sequence
			outEvent <- e
		}
		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure : %w", err)
			return
		}
	}()
	return outEvent, outError
}
