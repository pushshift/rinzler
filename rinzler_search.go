package rinzler

import (
    "os"
    "github.com/hashicorp/golang-lru"
    "strings"
    )

type BinarySearch struct {
    fp              *os.File
    cache           *lru.Cache
    recordSize      uint64
    fieldSize       uint64
    numRecords      uint64
    cacheHits       uint64
    cacheMisses     uint64
}

func (b *BinarySearch) CachePerformance() float64 {
    return float64(b.cacheHits) / float64(b.cacheMisses) * 100
}

func (r *Rinzler) NewBinarySearch(filename string, recordSize uint64, fieldSize uint64) *BinarySearch {
    var search BinarySearch
    search.fp,_ = os.OpenFile(filename, os.O_RDONLY, 0644)
    search.recordSize = recordSize
    search.fieldSize = fieldSize
    search.cache,_ = lru.New(100000)
    fi, _ := search.fp.Stat()
    fileSize := fi.Size()
    search.numRecords = uint64(fileSize) / search.recordSize
    return &search
}

func (b *BinarySearch) Search(target string) int64 {
    value,_ := b.cache.Get(target)
    if value != nil {
        b.cacheHits++
        return value.(int64)
    }
    b.cacheMisses++
    record := make([]byte,b.fieldSize)
    var min int64 = 0
    max := int64(b.numRecords)
    target = strings.ToLower(target)
    for min <= max {
        mean := int64((min + max) / 2)
        b.fp.Seek(int64(uint64(mean) * b.recordSize),os.SEEK_SET)
        _,err := b.fp.Read(record)
        if err != nil {
            panic(err)
        }
        str := strings.TrimRight(string(record),"\x00")
        str = strings.ToLower(str)
        if target == str {
            b.cache.Add(target,mean)
            return int64(mean)
        } else if target > str {
            min = mean + 1
        } else if target < str {
            max = mean - 1
        }
    }
    b.cache.Add(target,-1)
    return -1
}

func (b *BinarySearch) SearchLeft(target string) int64 {
    value,_ := b.cache.Get(target)
    if value != nil {
        b.cacheHits++
        return value.(int64)
    }
    b.cacheMisses++
    record := make([]byte,b.fieldSize)
    var min int64 = 0
    max := int64(b.numRecords)
    target = strings.ToLower(target)
    var str string
    var match bool
    for min < max {
        mean := int64((min + max) / 2)
        b.fp.Seek(int64(uint64(mean) * b.recordSize),os.SEEK_SET)
        _,err := b.fp.Read(record)
        if err != nil {
            panic(err)
        }
        str = strings.TrimRight(string(record),"\x00")
        str = strings.ToLower(str)
        if target == str {
            match = true
            max = mean
        } else if target < str {
            max = mean
        } else {
            min = mean + 1
        }
    }
    if match {
        b.cache.Add(target,min)
        return min
    }
    b.cache.Add(target,-1)
    return -1
}

func (b *BinarySearch) SearchRight(target string) int64 {
    value,_ := b.cache.Get(target)
    if value != nil {
        b.cacheHits++
        return value.(int64)
    }
    b.cacheMisses++
    record := make([]byte,b.fieldSize)
    var min int64 = 0
    max := int64(b.numRecords)
    target = strings.ToLower(target)
    var str string
    var match bool
    for min < max {
        mean := int64((min + max + 1) / 2)
        b.fp.Seek(int64(uint64(mean) * b.recordSize),os.SEEK_SET)
        _,err := b.fp.Read(record)
        if err != nil {
            panic(err)
        }
        str = strings.TrimRight(string(record),"\x00")
        str = strings.ToLower(str)
        if target == str {
            match = true
            min = mean
        } else if target > str {
            min = mean
        } else {
            max = mean - 1
        }
    }
    if match {
        b.cache.Add(target,max)
        return max
    }
    b.cache.Add(target,-1)
    return -1
}
