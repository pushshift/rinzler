package rinzler

import (
    "os"
    "github.com/hashicorp/golang-lru"
    "strings"
    "encoding/binary"
    _"fmt"
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

func (r *Rinzler) SearchLeftB(target int) int64 {
    iStart := r.FileDescription.IndexStartPos
    //iEnd := r.FileDescription.IndexEndPos
    //fmt.Println("Index Start and End positions: ",iStart,iEnd)

    //value,_ := b.cache.Get(target)
    //if value != nil {
    //    b.cacheHits++
    //    return value.(int64)
    //}
    //b.cacheMisses++

    var recordLength uint64 = 13
    //record := make([]byte,b.fieldSize)
    record := make([]byte,recordLength)

    var min uint64 = 0
    max := uint64((r.FileDescription.IndexEndPos - r.FileDescription.IndexStartPos) / uint64(recordLength))
    //fmt.Println("Index Min and Max Records: ",min,max)

    //max := int64(b.numRecords)
    //target = strings.ToLower(target)
    //var str string
    var match bool

    for min < max {
        mean := uint64((min + max) / 2)
        r.DataFile.Seek(int64(iStart + (mean*recordLength)),os.SEEK_SET)
        _,err := r.DataFile.Read(record)
        val := binary.LittleEndian.Uint64(record[5:])
        zz := append(record[:5],[]byte{0,0,0}...)
        //fmt.Println(zz)
        pos := binary.LittleEndian.Uint64(zz)
        _ = pos
        if err != nil {
            panic(err)
        }
        //fmt.Println(pos,val)
        //str = strings.TrimRight(string(record),"\x00")
        //str = strings.ToLower(str)
        if target == int(val) {
            match = true
            max = mean
        } else if target < int(val) {
            max = mean
        } else {
            min = mean + 1
        }
    }
    if match {
        //b.cache.Add(target,min)
        return int64(min)
    }
    //b.cache.Add(target,-1)
    return -1
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
