![Rinzler](https://www.khwiki.com/images/thumb/2/20/Rinzler_KH3D.png/350px-Rinzler_KH3D.png)


Rinzler is a high performant big data indexing system that efficiently stores data using both compression and Reed Solomon FEC (Forward error correction).

Advantages of Rinzler:

- Random I/O Access through compressed data
- Two levels of error detection with the ability to do error correction for each row of data
- Variable adjustment of error correction parameters to increase redundancy for each record stored
- Creation of indexes on specific fields with ultra fast read capability (I/O bound in most cases)


