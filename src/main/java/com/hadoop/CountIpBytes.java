package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CountIpBytes extends Configured implements Tool {

	private final Logger log = Logger.getLogger(CountIpBytes.class);

	public static void main(String[] args) {
		Configuration defaultConf = new Configuration();
		defaultConf.set("mapred.textoutputformat.separator", ",");
		int res;
		try {
			res = ToolRunner.run(defaultConf, new CountIpBytes(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] arg0) throws Exception {
		String inDir = arg0[0];
		String outDir = arg0[1];

		log.info("INPUT PATH: " + inDir);
		log.info("OUTPUT PATH: " + outDir);

		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf);
		job.setJarByClass(CountIpBytes.class);
		FileInputFormat.addInputPath(job, new Path(inDir));
		FileOutputFormat.setOutputPath(job, new Path(outDir));
		job.setMapperClass(CountIpBytesMapper.class);
		// job.setCombinerClass(cls);
		job.setReducerClass(CountIpBytesReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
