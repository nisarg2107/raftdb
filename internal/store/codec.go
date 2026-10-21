package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
)

// A versioned, length-prefixed envelope avoids JSON work for new log entries.
// Legacy single-command JSON is still accepted when replaying existing stores.
var commandMagic = []byte{'R', 'D', 'B', 1}

const MaxBatchCommands = 1024

func Encode(c Command) ([]byte, error) { return EncodeBatch([]Command{c}) }

func EncodeBatch(commands []Command) ([]byte, error) {
	if len(commands) == 0 || len(commands) > MaxBatchCommands {
		return nil, errors.New("invalid command batch size")
	}
	size := 6
	for _, c := range commands {
		if err := c.Validate(); err != nil {
			return nil, err
		}
		size += 7 + len(c.Key) + len(c.Value)
	}
	b := make([]byte, 0, size)
	b = append(b, commandMagic...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(commands)))
	for _, c := range commands {
		op := byte(1)
		if c.Op == "delete" {
			op = 2
		} else if c.Op == "fence" {
			op = 3
		}
		b = append(b, op)
		b = binary.BigEndian.AppendUint16(b, uint16(len(c.Key)))
		b = binary.BigEndian.AppendUint32(b, uint32(len(c.Value)))
		b = append(b, c.Key...)
		b = append(b, c.Value...)
	}
	return b, nil
}

func Decode(b []byte) (Command, error) {
	commands, err := DecodeBatch(b)
	if err != nil {
		return Command{}, err
	}
	if len(commands) != 1 {
		return Command{}, errors.New("expected one command")
	}
	return commands[0], nil
}

func DecodeBatch(b []byte) ([]Command, error) {
	if !bytes.HasPrefix(b, commandMagic) {
		var c Command
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, err
		}
		if err := c.Validate(); err != nil {
			return nil, err
		}
		return []Command{c}, nil
	}
	bad := errors.New("invalid binary command batch")
	if len(b) < 6 {
		return nil, bad
	}
	count := int(binary.BigEndian.Uint16(b[4:6]))
	if count == 0 || count > MaxBatchCommands {
		return nil, bad
	}
	b = b[6:]
	commands := make([]Command, 0, count)
	for i := 0; i < count; i++ {
		if len(b) < 7 {
			return nil, bad
		}
		op := b[0]
		keySize, valueSize := int(binary.BigEndian.Uint16(b[1:3])), int(binary.BigEndian.Uint32(b[3:7]))
		b = b[7:]
		if keySize > 1024 || valueSize > 256*1024 || keySize+valueSize > len(b) {
			return nil, bad
		}
		c := Command{Key: string(b[:keySize]), Value: string(b[keySize : keySize+valueSize])}
		switch op {
		case 1:
			c.Op = "put"
		case 2:
			c.Op = "delete"
		case 3:
			c.Op = "fence"
		default:
			return nil, bad
		}
		if err := c.Validate(); err != nil {
			return nil, err
		}
		commands = append(commands, c)
		b = b[keySize+valueSize:]
	}
	if len(b) != 0 {
		return nil, bad
	}
	return commands, nil
}
