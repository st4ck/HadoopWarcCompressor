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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class secondReducer extends Reducer<Text, Text, Text, Text>
{
	private static final int requestedPrecision = 15;
		
	private HashMap<String, Integer> precision;
	// using a sorted list to reduce contains() function
	// complexity from O(n) of ArrayList to O(log(n))
	// speeding up the entire phase
	private NaturalSortedList<String> alreadySeen;
	private MultipleOutputs<Text, Text> out;
	protected void setup(Context context) throws IOException, InterruptedException
	{
		out = new MultipleOutputs<Text, Text>(context);
		precision = new HashMap<String, Integer>();
		alreadySeen = new NaturalSortedList<String>();
	}
					 
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String keyString = key.toString();
		
		// filling list alreadySeen with all offsets
		if (keyString.equals("Z")) {
			for (Text val : values) {
				String valString = val.toString();
				if (!alreadySeen.contains(valString)) {
					precision.put(valString, 0);
					alreadySeen.add(valString);
				}
			}
		} else {
			for (Text val : values) {
				String valString = val.toString();
				// counting the number of occurrences offset1_offset2 and
				// save in the list precision
				if (precision.containsKey(keyString + "_" + valString)) {
					precision.put(keyString + "_" + valString, precision.get(keyString + "_" + valString)+1);
				} else {
					precision.put(keyString + "_" + valString, 1);
				}
				
				if (!alreadySeen.contains(keyString)) {
					alreadySeen.add(keyString);
				}
				
				if (!alreadySeen.contains(valString)) {
					alreadySeen.add(valString);
				}
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		Iterator it = precision.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<String, Integer> entry = (Map.Entry)it.next();
			
			Integer prec = entry.getValue();
			// if the item in precision has precision >= of the requested precision
			// save to the file similar
			if (prec >= requestedPrecision) {
				out.write(new Text(entry.getKey()), new Text(prec.toString()), "0_similar");
			} else if (prec == 0) {
				// if page is not similar to any other page save to file single
				out.write(new Text(entry.getKey()), new Text("0"), "2_single");
			} else {
				// otherwise (precision < req. precision) save to file alone
				out.write(new Text(entry.getKey()), new Text("1"), "1_alone");
			}
		}
		
		out.close();
	}
	
	
}