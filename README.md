# HadoopWarcCompressor
Compress large WARC (Web ARChive) file with LZMA2 (using java XZ library) after sorting pages by similarity

Comparison between pages inside WARC have done shingling the page (splitting into phrases the body excluding html tags), generating rolling hashes of each shingle and doing min-hashing

After that, similar pages are clustered and compressed together to increase compression ratio


## Usage
Compilate using mvn with

    mvn package
  

After that place you WARC input file in a HDFS directory (ex. "input/test.warc") and create an output directory (ex. "output/")
To start the compressor

    hadoop jar target/WarcCompressor-1.0.jar warccompressor.WarcCompressor input/test.warc output
  
  
The compressed output files (files number depending on the number of reducer) will be available in the directory output/compressed

## Tests & results
All tests have done under a Virtual Machine with 4GB of RAM and 2 vCPU, so with a single Hadoop node

The results

    File size: 47gb
    Gain in compression: 4.5% (respect of original file compressed without page sorting)
    Time:
      1° phase (shingling, fingerprinting, minhashing): about 10 hours
      2° phase (comparison): 5 minutes
      3° phase (clustering): 15 minutes
      4° phase (writing): about 16 hours
 
Reading and writing under VM using an HDD (not SSD) is very slow, so probably in the first and fourth fase the performance
could be better on physical machine with better disks, more RAM and more cores
XZ-LZMA2 compression is slow too, with GZip compression the writing time passes from 16 hours to about 12 hours

## Author
Developed by Roberto Tacconelli and distributed under GPLv3
