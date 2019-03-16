package rinzler


import (
    "github.com/vivint/infectious"
    "github.com/valyala/gozstd"
    "strconv"
    "encoding/binary"
    "hash/crc32"
    "errors"
)

// Calculate an 8 bit checksum from the 8 LSB bits of a CRC32 checksum
func (r *Rinzler) Checksum8(bs []byte) uint8 {
    crc := crc32.Checksum(bs,r.Crc32table)
    return uint8(crc & ((1 << 8) - 1))
}

// Calculate a 16 bit checksum from the 16 LSB bits of a CRC32 checksum
func (r *Rinzler) Checksum16(bs []byte) uint16 {
    crc := crc32.Checksum(bs,r.Crc32table)
    return uint16(crc & ((1 << 16) - 1))
}

// Calculate a Castagnoli CRC32 (Optimized for x86 SSE4.2 capable processors)
func (r *Rinzler) Checksum32(bs []byte) uint32 {
    crc := crc32.Checksum(bs,r.Crc32table)
    return crc
}

// Apply zstandard (zstd) compression to a byte slice
func (r *Rinzler) Compress(bs []byte, use_dict bool) []byte {
    if use_dict {
        compressed := gozstd.CompressDict(nil,bs,r.ZstdCDict)
        return compressed
    }
    compressed := gozstd.Compress(nil,bs)
    return compressed
}

// Decompress a zstandard (zstd) compressed byte slice
func (r *Rinzler) Decompress(bs []byte, use_dict bool) ([]byte, error) {
    if use_dict {
        decompressed,err := gozstd.DecompressDict(nil,bs,r.ZstdDDict)
        return decompressed,err
    }
    decompressed,err := gozstd.Decompress(nil,bs)
    return decompressed,err
}

// This function wraps the compression and Reed Solomon encoding functions and
// creates a compressed record with error detection and correction capabilities
func (r *Rinzler) CreateRecord(bs []byte) []byte {
    compressed := r.Compress(bs,true)
    encoded,_ := r.RSEncode(compressed,r.TotalSegments,r.ChecksumSegments,true)
    return encoded
}

// This function is currently unavailable (in progress...) 
func ReedSolomonCorrect(arr []byte, checksumSize ...int) error {
    checksumBytes := 2
    if len(checksumSize) > 0 {
        checksumBytes = checksumSize[0]
    }
    l := len(arr) - checksumBytes
    required, total := l, l+checksumBytes
    f, err := infectious.NewFEC(required, total)
    shares := make([]infectious.Share, total)
    for i := 0; i < total; i++ {
        shares[i].Number = i
        shares[i].Data = []byte{arr[i]}
    }
    err = f.Correct(shares)
    return err
}

// Decode a Reed Solomon Encoded byte string. This method will first check the available 8 bit
// checksum and return the record if the checksum matches the calculated checksum. Otherwise, corruption
// is assumed and the record is processed using the Berlekamp-Welch algorithm to detect the corrupted bits
// and repair the record
func (r *Rinzler) RSDecode(arr []byte, totalSegments uint8, checksumSize uint8) ([]byte, error) {
    const byteChecksum = 1
    const lengthMarker = 2
    const reservedBytes = 2
    // First we'll check the byte checksum and if it matches, the data is most likely not corrupt
    // This saves a lot of time from having to run through the Berlekamp-Welch check
    calculatedChecksum := r.Checksum8(arr[byteChecksum:])
    originalChecksum, arr := arr[0], arr[byteChecksum:]
    originalLength := binary.LittleEndian.Uint16(arr[0:lengthMarker])
    if calculatedChecksum == originalChecksum {
        return arr[lengthMarker+reservedBytes:originalLength+lengthMarker+reservedBytes],nil
    }
    checksumSegments := int(checksumSize)
    segmentLength := len(arr) / int(totalSegments)
    f, err := infectious.NewFEC(int(totalSegments)-checksumSegments, int(totalSegments))
    shares := make([]infectious.Share, totalSegments)
    for i := 0; i < int(totalSegments); i++ {
        shares[i].Number = i
        data := make([]byte,segmentLength)
        for j:= 0; j < int(segmentLength);j++ {
            pos := i*int(segmentLength) + j
            data[j] = arr[pos]
        }
        shares[i].Data = data
    }
    result, err := f.Decode(nil, shares)
    if err != nil {
        panic(err)
    }
    originalLength = binary.LittleEndian.Uint16(arr[0:lengthMarker])
    return result[lengthMarker+reservedBytes:originalLength+lengthMarker+reservedBytes], err
}

// This method encodes a byte string using Reed Solomon FEC. This adds redundant data so that error correction
// is possible if the record's data becomes corrupted.
func (r *Rinzler) RSEncode(arr []byte, totalSegments int, checksumSegments int, pad bool) ([]byte, error) {
    const byteChecksum = 1
    const lengthMarker = 2
    const reservedBytes = 2
    originalLength := len(arr)
    prependedData := make([]byte,4)
    binary.LittleEndian.PutUint16(prependedData[:2],uint16(originalLength))
    arr = append(prependedData,arr...)
    arrLength := len(arr)
    requiredSegments := totalSegments - checksumSegments
    stringAlignment := requiredSegments - (arrLength % requiredSegments) // alignment needs to be 0 or padded to get to 0
    arrLengthWithPadding := arrLength
    if pad && stringAlignment > 0 {
        padding := make([]byte, stringAlignment)
        arr = append(arr, padding...)
        arrLengthWithPadding += stringAlignment
    }

    if stringAlignment > 0 && !pad {
        err := errors.New("The length of the string passed must be a multiple of " + strconv.Itoa(requiredSegments) + ". (Off by " + strconv.Itoa(stringAlignment) + ")")
        return nil, err
    }
    f, err := infectious.NewFEC(requiredSegments, totalSegments)
    if err != nil {
        return nil, err
    }
    shares := make([]infectious.Share, totalSegments)
    encoded := make([]byte, int(totalSegments) * (arrLengthWithPadding / requiredSegments))
    output := func(s infectious.Share) {
        shares[s.Number] = s
        for idx, data := range s.Data {
            pos := s.Number * (arrLengthWithPadding / requiredSegments) + idx
            encoded[pos] = data
        }
    }
    err = f.Encode(arr, output)
    encoded = append([]byte{r.Checksum8(encoded)},encoded...)
    return encoded, err
}
