
# Rinzler

**Highly redundant compressed big data record storage and retrieval system.**

![Rinzler](https://www.khwiki.com/images/thumb/2/20/Rinzler_KH3D.png/350px-Rinzler_KH3D.png)


Rinzler is a high performant big data indexing system that efficiently stores data using both compression and Reed Solomon FEC (Forward error correction) and the Berlekamp-Welch error correction algorithm.

Capabilities of Rinzler:

- Random I/O Access through compressed data
- Two levels of error detection with the ability to do error correction for each row of data
- Variable adjustment of error correction parameters to increase redundancy for each record stored
- Creation of indexes on specific fields with ultra fast read capability (I/O bound in most cases)

## What was the reason for creating Rinzler?

Working with big data presents many challenges. I wanted to learn how to program in Golang, so I picked a project that would help make dealing with Big Data fun and exciting. Rinzler is a program with my different purposes but everything at the heart of the program is designed from the ground up to be a Big Data management tool.

There are several major components of Rinzler that help facilitate working with Big data. I'll review each major component below.

### Redundant Storage

Rinzler uses an advanced library for managing records of data. Each chunk of data is encoded using Reed Solomon FEC with a default setting of two redundant error detection and correction shards. Each row of data can withstand several bit flips and other types of corruption and still be recovered. Using FEC with the Berlekamp-Welch error correction algorithm can be CPU intensive when dealing with high sustained I/O, so there is an additional one byte checksum for each data record. This checksum is computed from a CRC32 checksum that has support under SSE4.2 CPU extensions. The 8 LSB bits from the CRC32 checksum is recorded as a one byte checksum for the data record. During reads, if the checksum is correct, the data does not need to be passed through the Reed Solomon decode method. 

These two redundant checks allow for extremely fast IO while also still providing strong protection against bit rot and other data degradation.

### Random Access Compression

Rinzler strives to give the best of both worlds with regards to compression. Generally, data that is compressed does not allow for efficient random record retrieval. Rinzler employs zstandard (zst) compression and gives the end-user the capability of creating custom compression dictionaries to efficiently compress small objects (usually objects less than 5 kilobytes). During testing and benchmarking, Rinzler typically had compression ratios of 4-5x (including the redundant packets of data for error recovery!). Using Twitter JSON blobs, Rinzler approaches compression levels of 5x using custom dictionaries.
