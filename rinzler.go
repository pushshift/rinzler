package rinzler

import (
    "fmt"
    "strings"
    "bufio"
    "os"
    "io"
    "bytes"
    "encoding/binary"
    "github.com/json-iterator/go"
    "strconv"
    "sort"
    "time"
    "github.com/valyala/gozstd"
    "hash/crc32"
    "io/ioutil"
)

type Rinzler struct {
    Crc32table              *crc32.Table
    ZstdCDict               *gozstd.CDict
    ZstdDDict               *gozstd.DDict
    ZstdCompressionLevel    int
    TotalSegments           int
    ChecksumSegments        int
    ZstdDictionary          []byte
    ZstdMagicHeader         []byte
}

func New() *Rinzler {
    var dict, _ = ioutil.ReadFile("/dev/shm/json/dictionary")
    rinzler := Rinzler{
        Crc32table : crc32.MakeTable(crc32.Castagnoli),
        ZstdCompressionLevel : 5,
        ZstdMagicHeader : []byte{40,181,47,253},
        ZstdDictionary : dict,
    }
        rinzler.ZstdCDict,_ = gozstd.NewCDictLevel(rinzler.ZstdDictionary,rinzler.ZstdCompressionLevel)
        rinzler.ZstdDDict,_ = gozstd.NewDDict(rinzler.ZstdDictionary)
        rinzler.TotalSegments = 20
        rinzler.ChecksumSegments = 2
    return &rinzler
}

type author struct {
    Id uint64
    Created_utc uint32
    Name string
//    NameLowercase string
}

type subreddit struct {
    Id uint64
    Name string
}

type record struct {
    Id uint64
    Subreddit_id uint64
    Position uint64
}

type CommentJSON struct {
    Id                  string  `json:"id"`
    Subreddit           string  `json:"subreddit"`
    Author              string  `json:"author"`
    Author_fullname     string  `json:"author_fullname"`
    Link                string  `json:"link_id"`
    Permalink           string  `json:"permalink"`
    Subreddit_id        string  `json:"subreddit_id"`
    Score               int32   `json:"score"`
    Created_utc         uint32  `json:"created_utc"`
    Retrieved_on        uint32  `json:"retrieved_on"`
    Author_created_utc  uint32  `json:"author_created_utc"`
    //Link_id uint64
}

func getTime() {
const epoch = 1234567890
const layout = "2006-01-02-%d.txt"
    t := time.Unix(epoch,0)
    fmt.Println("The time was", t.Format(time.RFC822Z))
    fmt.Println(t.Month())
    fmt.Println(fmt.Sprintf(t.Format(layout), 4))
}

/*
func read(ps *pushshift.Pushshift) {
    //var compressed []byte
    var decompressed []byte

    z, _ := os.OpenFile("/tank/data.idx", os.O_RDWR | os.O_CREATE, 0644)
    m, _ := os.OpenFile("/tank/data.cmp", os.O_RDWR | os.O_CREATE, 0644)

    buf := make([]byte, 1 << 12)
    buf2 := make([]byte, 2)
    var counter int = 0
    reader := bufio.NewReader(z)

    for {
        bytes_read, err := reader.Read(buf2)
        fmt.Println(bytes_read,buf2)
        os.Exit(0)
        if err != nil {
            if err == io.EOF {
                break
            } else {
                fmt.Println("Error: " + err.Error())
                os.Exit(1)
            }
        }
        counter++

        for i := 0; i < bytes_read; i+=2 {
            u := binary.LittleEndian.Uint16(buf[i:i+2])
            //u := binary.LittleEndian.Uint64(append(bs64[:5],0,0,0))
            buf2 := make([]byte,u)
            if _, err := io.Reader.Read(m, buf2); err != nil {
                fmt.Println("Error: " + err.Error())
                os.Exit(1)
            } else {
                decompressed,_ = ps.Decompress(buf2,true)
                fmt.Println(string(decompressed))
            }
        }
    }
    return
}
*/
func seek() {
    f,_ := os.OpenFile("/dev/shm/reddit_subreddit.dat", os.O_RDONLY | os.O_CREATE, 0644)
    //r := bufio.NewReader(f)
    record := make([]byte,22)
    for i:= 0; i< 300; i+=30 {
    _, _ = f.Seek(int64(i),os.SEEK_SET)
    f.Read(record)
    fmt.Println(string(record))
    }
}

func main() {

    rinz := New()

    searcher := rinz.NewBinarySearch("/dev/shm/reddit_subreddit.dat",30,22)
    _ = searcher
    //loc := b.Search("askreddit")
    //fmt.Println(loc)

    //s.Subreddits = make(map[string]int)
    //s.Subreddits["a"] = 3
    //s.Subreddits["b"] = 4
    //fmt.Println(s)

    //var pp = make(map[string]int)
    //pp["a"] = 3
    //pp["b"] = 4
    //fmt.Println(pp)

    //return
    //seek()
    //return
    //i := binarySearch("AskReddit")
    //fmt.Println(i)
    //return
/*
    ss := []byte("This is a test. This is only a test. This is a GREAT test to see what we can do here!")

    encoded,_ := ps.RSEncode(ss,10,2,true)
    decoded,_ := ps.RSDecode(encoded,10,2)
    print(string(decoded))

    return
*/
    compressedTotal, uncompressedTotal := 0,0
    const layout = "2006-01-31"
    fs := make(map[string]*os.File,10)
    //dat := make(map[string]*os.File,10)
    //index := make(map[string]*os.File,10)
    t := time.Unix(0,0)
    t.Format("2019-01-31")
    var json = jsoniter.ConfigCompatibleWithStandardLibrary
    input := bufio.NewReader(os.Stdin)
    bs64 := make([]byte,8)
    var l int = 0
    var records []record
    authors := make(map[string]*author,100000)
    subreddits := make(map[string]*subreddit,100000)
    var pos int64
    var max_author_length int = 0
    var max_subreddit_length int = 0
    var found int = 0
    for {
        comment := CommentJSON{}
        line, err := input.ReadBytes('\n')
        if err != nil {
            if err == io.EOF {
                break
            } else {
            fmt.Fprintln(os.Stderr, "Error reading standard input:", err)
            break
            }
        } else {
        src := line[:len(line) - 1]
        src = bytes.TrimSuffix(line, []byte("\n"))
        err := json.Unmarshal(line, &comment)
        if err != nil {
            fmt.Println(err)
            os.Exit(0)
        }

        var created_utc int64 = int64(comment.Created_utc)
        t := time.Unix(created_utc,0)
        t_string := fmt.Sprintf("%04d-%02d-%02d",t.Year(),t.Month(),t.Day())

        if _,exists := fs[t_string+"dat"]; !exists {
            fs[t_string+"dat"],_ = os.OpenFile("/dev/shm/" + t_string + ".dat", os.O_RDWR |os.O_CREATE | os.O_TRUNC, 0644)
            fs[t_string+"idx"],_ = os.OpenFile("/dev/shm/" + t_string + ".idx", os.O_RDWR |os.O_CREATE | os.O_TRUNC, 0644)
            pos,_ = fs[t_string+"dat"].Seek(0,os.SEEK_END)
            fs[t_string+"idx"].Seek(0,os.SEEK_END)
        }

        var subreddit_id uint64 = 0
        if comment.Subreddit_id != "" {
            val,_ := strconv.ParseUint(comment.Subreddit_id[3:],36,32)
            subreddit_id = uint64(val)
        }

        if len(comment.Subreddit) > max_subreddit_length {max_subreddit_length = len(comment.Subreddit)}
        if len(comment.Author) > max_author_length {max_author_length = len(comment.Author)}

        var author_id uint64 = 0
        if comment.Author_fullname != "" {
            val,_ := strconv.ParseInt(comment.Author_fullname[3:],36,64)
            author_id = uint64(val)
        }
        test := searcher.SearchRight(comment.Subreddit);if test != -1 {found++} else {fmt.Println("Not found: ", comment.Subreddit)}
        var author_created_utc uint32 = uint32(comment.Author_created_utc)
        authors[comment.Author] = &author{Id:author_id,Name:comment.Author,Created_utc:author_created_utc}
        subreddits[comment.Subreddit] = &subreddit{Id:subreddit_id,Name:comment.Subreddit}
        id,_ := strconv.ParseUint(comment.Id,36,64)
        records = append(records,record{Id:id,Subreddit_id:subreddit_id,Position:uint64(pos)})
        compressed := rinz.Compress(src,true)
        compressed = compressed[4:]
        encoded,_ := rinz.RSEncode(compressed,18,2,true)
        l = len(encoded)
        //decoded,_ := ps.RSDecode(encoded,18,2)
        uncompressedTotal += len(line)
        compressedTotal += l
        fs[t_string+"dat"].Write(encoded)
        binary.LittleEndian.PutUint64(bs64,uint64(uint64(pos)))
        fs[t_string+"idx"].Write(bs64[:5])
        pos += int64(l)
        }
    }
    fmt.Println("Found: ",found)
    fmt.Printf("Cache Performance: %.2f%%\n",searcher.CachePerformance())
    fmt.Println("Ratio: ",float64(compressedTotal) / float64(uncompressedTotal))
    fmt.Println("Max Author Length: ", max_author_length)
    fmt.Println("Max Subreddit Length: ", max_subreddit_length)

    sort.Slice(records, func(i, j int) bool {
        return records[i].Subreddit_id < records[j].Subreddit_id
    })


//    fmt.Println(records)
    var author_slice []author

    for _, v := range authors {
        val := *v
        author_slice = append(author_slice,val)
    }

    sort.Slice(author_slice, func(i, j int) bool {
        return strings.ToLower(author_slice[i].Name) < strings.ToLower(author_slice[j].Name)
    })

    aFile, _ := os.OpenFile("/dev/shm/reddit_author.dat", os.O_RDWR | os.O_CREATE, 0644)
    aFile.Seek(0,os.SEEK_END)

    for _, v := range author_slice {
        name := make([]byte,20)
        copy(name,v.Name)
        aFile.Write(name)
        id := make([]byte,8)
        binary.LittleEndian.PutUint64(id,v.Id)
        aFile.Write(id)
        created_utc := make([]byte,4)
        binary.LittleEndian.PutUint32(created_utc,v.Created_utc)
        aFile.Write(created_utc)
    }
    aFile.Close()

//    var subreddit_slice []subreddit
//    for _, v := range subreddits {
//        val := *v
//        subreddit_slice = append(subreddit_slice,val)
//    }

//    sort.Slice(subreddit_slice, func(i, j int) bool {
//        return strings.ToLower(subreddit_slice[i].Name) < strings.ToLower(subreddit_slice[j].Name)
//    })

//    sFile, _ := os.OpenFile("/dev/shm/reddit_subreddit.dat", os.O_RDWR | os.O_CREATE, 0644)
//    sFile.Seek(0,os.SEEK_END)

//    for _, v := range subreddit_slice {
//        name := make([]byte,22)
//        copy(name,v.Name)
//        sFile.Write(name)
//        id := make([]byte,8)
//        binary.LittleEndian.PutUint64(id,v.Id)
//        sFile.Write(id)
//    }
//    sFile.Close()
//    fmt.Println(subreddit_slice)


//    fmt.Println(author_slice)
    // Decompress a string
    //d,_ := ps.Decompress(append(ps.ZstdMagicHeader,b[4:]...),false)
    //fmt.Printf("%s",string(d))

    //s1 := "Bobby"
    //s2 := "obby."
    //s3 := "Bobby was in charge of engineering for three weeks."

    //for i:=0;i<len(s3)-5;i++ {
    //    fmt.Println(s3[i:i+5])
    //}

}
