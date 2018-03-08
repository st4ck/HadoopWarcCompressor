# HadoopWarcCompressor
Compress large WARC (Web ARChive) file with LZMA2 (using java XZ library) after sorting pages by similarity

Comparison between pages inside WARC have done shingling the page (splitting into phrases the body excluding html tags), generating rolling hashes of each shingle and doing min-hashing

After that, similar pages are clustered and compressed together to increase compression ratio


## Usage
The used version for this project is Hadoop 1.2.1[8] with only two changes in the mapred-size.xml configuration

I’ve added these two properties in the file to enable the compression between mapper and reducer to speed up the process and to enable GZip codec used in the project

    <property>
         <name>mapred.compress.map.output</name> 
         <value>true</value>
    </property>
    <property>
         <name>mapred.map.output.compression.codec</name>
         <value>org.apache.hadoop.io.compress.GzipCodec</value>
    </property>

To build the project use maven and run

    mvn package

After that place you WARC input file in a HDFS directory (ex. "input/test.warc") and create an output directory (ex. "output/")

To start the compressor

    hadoop jar target/WarcCompressor-1.0.jar warccompressor.WarcCompressor input/test.warc output hadoop

The first arg “input/test.warc” represents relative path inside of HDFS of the original warc file

The second arg “output” is the relative destination directory inside of HDFS

The third arg “hadoop" is the HDFS username current keeping the WARC file (used to obtain the HDFS path /user/username/input/… )

The compressed output files (files number depending on the number of reducer) will be available in the directory output/compressed

Three other directories are created inside the output directory called firstphase, secondphase, thirdphase respectively containing the intermediate data between phases.


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
 
Reading and writing under VM using an HDD (not SSD) is very slow, so probably in the first and fourth phase the performance
could be better on physical machine with better disks, more RAM and more cores
XZ-LZMA2 compression is slow too, with GZip compression the writing time passes from 16 hours to about 12 hours

## Author
Developed by Roberto Tacconelli and distributed under GPLv3
