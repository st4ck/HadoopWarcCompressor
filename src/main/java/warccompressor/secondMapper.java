/*
This file is part of Hadoop WARC Compressor.

Hadoop WARC Compressor is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Hadoop WARC Compressor is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Hadoop WARC Compressor.  If not, see <http://www.gnu.org/licenses/>.
*/

package warccompressor;

import java.io.IOException;
import java.math.*;
import java.util.*;
import java.lang.StringBuilder;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class secondMapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable positionOffset, Text hashAndOffset, Context context) throws IOException, InterruptedException
	{
		// input format: <minhash offset1,offset2,offset3...>
		String[] hno = hashAndOffset.toString().split("\t");
		String hash = hno[0];
		String[] offsetsStr = hno[1].split(",");
		
		// java automatically strips last element if empty (after last ,)
		int offsetsCount = offsetsStr.length;
		
		if (offsetsCount == 1) {
			// unique minhash, no similar pages with this minhash
			// sending with key "Z" to group pages not similar to others
			context.write(new Text("Z"), new Text(offsetsStr[0]));
		} else if (offsetsCount > 1) {
			// more or equal than 2 pages with same minhashs
			BigInteger[] offsets = new BigInteger[offsetsCount];
			
			// finding minimum minhash
			offsets[0] = new BigInteger(offsetsStr[0]);
			BigInteger minOffset = offsets[0];
			String minOffsetStr = offsetsStr[0];
			for (int i=1; i<offsetsCount; i++) {
				offsets[i] = new BigInteger(offsetsStr[i]);
				if (minOffset.compareTo(offsets[i]) > 0) {
					minOffset = offsets[i];
					minOffsetStr = offsetsStr[i];
				}
			}
			
			Text minOffsetText = new Text(minOffsetStr);
			
			// sending only pair minimum offset / offset[i]
			
			for (int i=0; i<offsetsCount; i++) {
				if (!minOffset.equals(offsets[i])) {
					context.write(minOffsetText, new Text(offsetsStr[i]));
				}
			}
			
			/*
			sending each pair offset1_offset2 is not necessarely
			~O(n^2), too much memory, too much time, no gain in compression
			
			for (int i=0; i<offsetsCount; i++) {
				offsets[i] = new BigInteger(offsetsStr[i]);
			}
			
			for (int i=0; i<offsetsCount-1; i++) {
				Text oi = new Text(offsetsStr[i]);
				for (int j=i+1; j<offsetsCount; j++) {
					int cmp = offsets[i].compareTo(offsets[j]);
					switch (cmp) {
						case -1:
							context.write(oi, new Text(offsetsStr[j]));
							break;
						case 1:
							context.write(new Text(offsetsStr[j]), oi);
							break;
						default:
					}
				}
			}*/
		}
	}
}