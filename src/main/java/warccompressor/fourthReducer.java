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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.nio.charset.StandardCharsets;

public class fourthReducer extends Reducer<groupOffsetPair, Text, Text, Text>
{
	//private HashMap<String,Integer> precision;
	private FSDataInputStream fsDataInputStream;
	
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		String warcfilepath = conf.get("warcfilepath");
		FileSystem fs = FileSystem.get(conf);
		conf.addResource(new Path("core-site.xml"));
		
		String Filename = "/user/hadoop/"+warcfilepath;
			
		fsDataInputStream = fs.open(new Path(Filename));
	}
	
	public void reduce(groupOffsetPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		// receiving <cluster number, List<offsets>>
		// offsets are ordered with secondary sorting
		
		//String keyString = key.toString();
		
		for (Text val : values) {
			String valString = val.toString();

			long ByteOffset = Long.parseLong(valString);
			byte[] buffer = new byte[1024];
			String len = "";
			
			// find page length
			fsDataInputStream.read(ByteOffset, buffer, 0, 100);
			
			for (int i=0; i<100; i++) {
				if (buffer[i] == ' ') {
					break;
				}
				
				len += (char) (buffer[i] & 0xFF);
			}
			
			Integer lenL = Integer.parseInt(len);
			
			// Remove length of string "warc/0.9 " | the warc separator
			ByteOffset -= 9;
			
			int sk = 0;
			StringBuilder page = new StringBuilder("");
			for (sk=0; sk<lenL; sk+=1024) {
				int cread = -1;
				
				if (lenL-sk < 1024) {
					cread = fsDataInputStream.read(ByteOffset+sk, buffer, 0, lenL-sk);
				} else {
					cread = fsDataInputStream.read(ByteOffset+sk, buffer, 0, 1024);
				}
				
				if (cread > 0) {
					page.append((new String(buffer, StandardCharsets.ISO_8859_1)).substring(0,cread));
				}
			}
			
			context.write(new Text(page.toString()), new Text(""));
		}
	}
	
}