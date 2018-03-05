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
import org.apache.hadoop.conf.Configured;   
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class WarcCompressor extends Configured implements Tool
{
	private static final int permNumber = 50;
	
	public int run(String[] args) throws Exception {
		JobControl jobControl = new JobControl("jobChain"); 
	
		Configuration conf1 = new Configuration(true);
		
		// generate #permNumber random seeds to produce same random classes in different nodes
		Random seedGenerator = new Random();
		StringBuilder sharedSeeds = new StringBuilder();
		for (int i = 0; i < permNumber; i++) sharedSeeds.append(seedGenerator.nextLong() + ",");
		conf1.set("shared-seeds", sharedSeeds.toString().substring(0, sharedSeeds.toString().length() - 1));
		
		Job job1 = Job.getInstance(conf1);  
		job1.setJobName("WarcCompressor 1st phase - shingling, fingerprinting and minhashing");
				
		job1.setJarByClass(WarcCompressor.class);
		
		job1.setInputFormatClass(WarcFileInputFormat.class);
		
		job1.setMapOutputValueClass(LongWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setMapperClass(firstMapper.class);
		job1.setReducerClass(firstReducer.class);
		
		job1.setNumReduceTasks(5);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/firstphase"));
		
		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);

		jobControl.addJob(controlledJob1);
		
		Configuration conf2 = new Configuration(true);

		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(WarcCompressor.class);
		job2.setJobName("WarcCompressor 2nd phase - comparison between minhashs");

		FileInputFormat.setInputPaths(job2, new Path(args[1] + "/firstphase"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/secondphase"));

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setMapperClass(secondMapper.class);
		job2.setReducerClass(secondReducer.class);
		
		job2.setNumReduceTasks(5);

		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(job2);
	
		// make job2 dependent on job1
		controlledJob2.addDependingJob(controlledJob1); 
		// add the job to the job control
		jobControl.addJob(controlledJob2);
		
		Configuration conf3 = new Configuration(true);
		
		Job job3 = Job.getInstance(conf3);
		job3.setJarByClass(WarcCompressor.class);
		job3.setJobName("WarcCompressor 3nd phase - clustering");

		FileInputFormat.setInputPaths(job3, new Path(args[1] + "/secondphase"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/thirdphase"));

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		job3.setMapperClass(thirdMapper.class);
		job3.setReducerClass(thirdReducer.class);
		
		job3.setNumReduceTasks(5);

		ControlledJob controlledJob3 = new ControlledJob(conf3);
		controlledJob3.setJob(job3);

		// make job3 dependent on job2
		controlledJob3.addDependingJob(controlledJob2); 
		// add the job to the job control
		jobControl.addJob(controlledJob3);
		
		Configuration conf4 = new Configuration(true);
		
		conf4.set("warcfilepath", args[0]);
				
		Job job4 = Job.getInstance(conf4);
		job4.setJarByClass(WarcCompressor.class);
		job4.setJobName("WarcCompressor 4th phase - compression");

		FileInputFormat.setInputPaths(job4, new Path(args[1] + "/thirdphase"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/compressed"));

		job4.setMapOutputKeyClass(groupOffsetPair.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setCompressOutput(job4, true);
		TextOutputFormat.setOutputCompressorClass(job4, XZCodec.class);
		
		job4.setMapperClass(fourthMapper.class);
		job4.setReducerClass(fourthReducer.class);
        job4.setPartitionerClass(fourthPartitioner.class);
		job4.setGroupingComparatorClass(fourthGroupingComparator.class);
		
		job4.setNumReduceTasks(5);

		ControlledJob controlledJob4 = new ControlledJob(conf4);
		controlledJob4.setJob(job4);

		// make job4 dependent on job3
		controlledJob4.addDependingJob(controlledJob3); 
		// add the job to the job control
		jobControl.addJob(controlledJob4);
		
		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();

		while (!jobControl.allFinished()) {
			int running = jobControl.getRunningJobList().size();
			int waiting = jobControl.getWaitingJobList().size();
			
			if (jobControl.getFailedJobList().size() == 0) {
				if ((running == 1) && (waiting == 3)) {
					System.out.println("Doing shingling, fingerprinting and minhashing..."); 
				} else if ((running == 1) && (waiting == 2)) {
					System.out.println("Doing comparison between minhashs..."); 
				} else if ((running == 1) && (waiting == 1)) {
					System.out.println("Doing clustering..."); 
				} else if ((running == 1) && (waiting == 0)) {
					System.out.println("Doing compression..."); 
				}
				
			} else {
				System.exit(1);  
				return 1;   
			}
			
			try {
				Thread.sleep(30000);
			} catch (Exception e) {

			}
		} 
		
		System.out.println("-----");
		System.out.println("Done! Output ready");
		
	    System.exit(0);  
	    return (job4.waitForCompletion(true) ? 0 : 1);   	

	}

	public static void main(String[] args) throws Exception
	{
		
		int exitCode = ToolRunner.run(new WarcCompressor(), args);  
		System.exit(exitCode);
  	}
}
