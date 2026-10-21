package store

import (
	"errors"
	"fmt"
)

const (
	MaxAPIBatchOperations = 128
	MaxAPIBatchBytes      = 256 * 1024
	MaxBatchResultBytes   = 1024 * 1024
)

var ErrInvalidBatch = errors.New("invalid batch")
var ErrBatchResultTooLarge = errors.New("batch read results exceed 1 MiB")

func ValidateKey(key string) error {
	if len(key) == 0 || len(key) > 1024 {
		return errors.New("key must be 1..1024 bytes")
	}
	return nil
}

// Batch validators are shared by internal callers and the gRPC boundary.
// Every entry is checked before the caller can enqueue any mutation.
func ValidateWriteBatch(commands []Command) error {
	if len(commands) < 1 || len(commands) > MaxAPIBatchOperations {
		return fmt.Errorf("%w: expected 1..128 operations", ErrInvalidBatch)
	}
	size := 0
	for _, command := range commands {
		if command.Op != "put" {
			return fmt.Errorf("%w: batch only accepts PUT", ErrInvalidBatch)
		}
		if err := command.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidBatch, err)
		}
		size += len(command.Key) + len(command.Value)
	}
	if size > MaxAPIBatchBytes {
		return fmt.Errorf("%w: request exceeds 256 KiB", ErrInvalidBatch)
	}
	return nil
}

func ValidateReadBatch(keys []string) error {
	if len(keys) < 1 || len(keys) > MaxAPIBatchOperations {
		return fmt.Errorf("%w: expected 1..128 keys", ErrInvalidBatch)
	}
	size := 0
	for _, key := range keys {
		if err := ValidateKey(key); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidBatch, err)
		}
		size += len(key)
	}
	if size > MaxAPIBatchBytes {
		return fmt.Errorf("%w: request exceeds 256 KiB", ErrInvalidBatch)
	}
	return nil
}

type Lookup struct {
	Found bool
	Value string
}

// GetMany observes one map version; strings remain immutable after unlocking.
func (f *FSM) GetMany(keys []string) ([]Lookup, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	results := make([]Lookup, len(keys))
	size := 0
	for i, key := range keys {
		value, found := f.values[key]
		size += len(value)
		if size > MaxBatchResultBytes {
			return nil, ErrBatchResultTooLarge
		}
		results[i] = Lookup{Found: found, Value: value}
	}
	return results, nil
}
