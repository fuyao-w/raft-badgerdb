package raft_badgerdb

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
	. "github.com/fuyao-w/common-util"
	raft "github.com/fuyao-w/go-raft"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	nameSpaceKV     = []byte("kv")
	kvNameSpaceLen  = len(nameSpaceKV)
	nameSpaceLog    = []byte("log")
	logNameSpaceLen = len(nameSpaceLog) + 8

	ErrKeyIsNil   = errors.New("key is nil")
	ErrValueIsNil = errors.New("value is nil")
	ErrRange      = errors.New("from must no bigger than to")
)

// ParseLogKey parses the index from the key bytes.
func parseLogKey(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[len(key)-8:])
}

func buildLogKey(k uint64) []byte {
	out := make([]byte, logNameSpaceLen)
	copy(out, nameSpaceLog)
	binary.BigEndian.PutUint64(out[len(nameSpaceLog):], k)
	return out
}

func buildKvKey(k []byte) []byte {
	out := make([]byte, kvNameSpaceLen+len(k))
	copy(out, nameSpaceKV)
	copy(out[kvNameSpaceLen:], k)
	return out
}

type Store struct {
	db *badger.DB
}

func SimpleBadgerOptions(path string, inMemory bool) badger.Options {
	if inMemory {
		return badger.DefaultOptions("").WithInMemory(inMemory)
	}
	return badger.DefaultOptions(path)
}
func NewStore(opts badger.Options) (*Store, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	store := &Store{db}
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			_ = db.RunValueLogGC(0.7)
		}
	}()
	return store, nil
}

func (s *Store) Get(key []byte) (val []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(buildKvKey(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return raft.ErrKeyNotFound
			}
			return err
		}
		val = make([]byte, item.ValueSize())
		val, err = item.ValueCopy(nil)
		return err
	})
	return
}

func (s *Store) Set(key []byte, val []byte) error {
	if len(key) == 0 {
		return ErrKeyIsNil
	}
	if len(val) == 0 {
		return ErrValueIsNil
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(buildKvKey(key), val)
	})
}

func (s *Store) SetUint64(key []byte, val uint64) error {
	if len(key) == 0 {
		return ErrKeyIsNil
	}
	return s.Set(buildKvKey(key), uint2Bytes(val))
}

func (s *Store) GetUint64(key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, ErrKeyIsNil
	}
	val, err := s.Get(buildKvKey(key))
	return bytes2Uint(val), err
}

func (s *Store) FirstIndex() (idx uint64, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.IteratorOptions{
			Prefix: nameSpaceLog,
		}
		iterator := txn.NewIterator(opts)
		defer iterator.Close()
		iterator.Seek(buildLogKey(0))
		if !iterator.Valid() {
			return nil
		}
		idx = parseLogKey(iterator.Item().KeyCopy(nil))
		return nil
	})
	return
}

func (s *Store) LastIndex() (idx uint64, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.IteratorOptions{
			Prefix:  nameSpaceLog,
			Reverse: true,
		}
		iterator := txn.NewIterator(opts)
		defer iterator.Close()
		iterator.Seek(buildLogKey(math.MaxUint64))
		if !iterator.Valid() {
			return nil
		}
		idx = parseLogKey(iterator.Item().KeyCopy(nil))
		return nil
	})
	return
}

func (s *Store) GetLog(index uint64) (log *raft.LogEntry, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(buildLogKey(index))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return raft.ErrKeyNotFound
			}
			return err
		}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &log)
		})
	})
	return
}

func (s *Store) GetLogRange(from, to uint64) (logs []*raft.LogEntry, err error) {
	if from > to {
		return nil, ErrRange
	}
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = nameSpaceLog
		iterator := txn.NewIterator(opts)
		defer iterator.Close()

		for iterator.Seek(buildLogKey(from)); iterator.Valid(); iterator.Next() {
			if parseLogKey(iterator.Item().Key()) > to {
				break
			}
			iterator.Item().Value(func(val []byte) error {
				var log raft.LogEntry
				err = msgpack.Unmarshal(val, &log)
				logs = append(logs, &log)
				return err
			})
		}
		return nil
	})
	return
}

func (s *Store) SetLogs(logs []*raft.LogEntry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, log := range logs {
			key := buildLogKey(log.Index)
			val, err := msgpack.Marshal(log)
			if err != nil {
				return err
			}
			if err = txn.Set(key, val); err != nil {
				return err
			}

		}
		return nil
	})
}

func (s *Store) DeleteRange(from, to uint64) error {
	if from > to {
		return ErrRange
	}
	return s.db.Update(func(txn *badger.Txn) error {
		for i := from; i <= to; i++ {
			if err := txn.Delete(buildLogKey(i)); err != nil {
				return err
			}
		}
		return nil
	})
}

func uint2Bytes(i uint64) []byte {
	return strconv.AppendUint([]byte(nil), i, 10)
}
func bytes2Uint(b []byte) uint64 {
	res, _ := strconv.ParseUint(Bytes2Str(b), 10, 64)
	return res
}

func (s *Store) Sync() error {
	return s.db.Sync()
}
func (s *Store) Close() error {
	return s.db.Close()
}
