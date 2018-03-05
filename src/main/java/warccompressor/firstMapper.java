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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class firstMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	private static final int permNumber = 50;
	
	private RandomPermutations[] permutation = new RandomPermutations[50];

	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String sharedSeeds = conf.get("shared-seeds");
		String[] sharedSeed = sharedSeeds.split(",");
		for (int i=0; i < permNumber; i++) {
			permutation[i] = new RandomPermutations(new Random(Long.parseLong(sharedSeed[i])));
		}
	}
	
	public void map(LongWritable positionOffset, Text warcPage, Context context) throws IOException, InterruptedException
	{
		if (positionOffset.get() == 0) return;
		
		String page = warcPage.toString();
		
		Shingle shingles = new Shingle(page);
		int shinglesCount = shingles.getShinglesCount();
		
		Fingerprint bodyFingers = new Fingerprint(shingles.getBodyShingles(),page,permNumber,context);
		
		BigInteger[] mins = new BigInteger[permNumber];

		for (int i = 0; i < permNumber; i++)
		{
			BigInteger bodymin = BigInteger.ZERO;
			bodymin = permutation[i].getMin(bodyFingers);
			mins[i] = bodymin;
			
			context.write(new Text(bodymin.toString()), positionOffset);
			
			if (bodymin.compareTo(BigInteger.ZERO) == 0) {
				break;
			}
		}
		
		//System.out.println("Position: "+positionOffset+" #shingles: "+shinglesCount+" #fingerprints: "+bodyFingers.getSize()+" firstMin: "+mins[0]);
		
		/*System.out.println("Position: "+key);
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
			break;
		}*/
	}
}