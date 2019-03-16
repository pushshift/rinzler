package rinzler

import (
    "testing"
    "encoding/binary"
    "bytes"
    _"fmt"
)


func TestRSEncode(t *testing.T) {

    rinz := New()
    testSlice := []byte("12345678901234567890123456789012345678901234567890")
    encoded,err := rinz.RSEncode(testSlice,10,2,true)

    // Check if any errors were returned from encoding function
    if err != nil {
        t.Errorf(err.Error())
    }

    // Checksum digit should match
    calculatedChecksum := rinz.Checksum8(encoded[1:])
    presentChecksum := encoded[0]
    if calculatedChecksum != presentChecksum {
        t.Errorf("Checksum from encoding operation doesn't match calculated checksum")
    }

    // Check if length bytes are correct
    calculatedLength := binary.LittleEndian.Uint16(encoded[1:3])
    testSliceLength := len(testSlice)
    if uint16(testSliceLength) != calculatedLength {
        t.Errorf("Calculated length from encoding function doesn't match actual length.")
    }
}

func TestRSDecode(t *testing.T) {

    rinz := New()
    testSlice := []byte("12345678901234567890123456789012345678901234567890")
    encoded,err := rinz.RSEncode(testSlice,10,2,true)

    decoded,err := rinz.RSDecode(encoded,10,2)

    if err != nil {
        t.Errorf(err.Error())
    }

    if ! bytes.Equal(decoded,testSlice) {
        t.Errorf("The decoded data does not match the original data")
    }
}

func TestCompressDecompressNoDict(t *testing.T) {

    rinz := New()
    str := []byte("This is an example of a string that will be compressed by zstd compression. I will not fail (I hope)")
    compressed := rinz.Compress(str,false)
    decompressed,err := rinz.Decompress(compressed,false)

    if err != nil {
        t.Errorf("Error during the decompression method")
    }

    if ! bytes.Equal(decompressed,str) {
        t.Errorf("The decoded data does not match the original data")
    }
}
