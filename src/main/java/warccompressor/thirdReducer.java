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

public class thirdReducer extends Reducer<Text, Text, Text, Text>
{	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		// receiving offset -> group
		
		String keyString = key.toString();
		
		String valString = values.iterator().next().toString();
		
		// find lowest group, cause values are not sorted
		// "alone" group is lexicographically greater than numbers of groups
		for (Text val : values) {
			String tempString = val.toString();
			if (valString.compareTo(tempString) > 0) {
				valString = tempString;
			}
		}

		context.write(new Text(keyString), new Text(valString));
	}
	
}