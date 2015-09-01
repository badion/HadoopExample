package com.hadoop;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class CountIpBytesReduce extends Reducer<Text, IntWritable, Text, Text> {

	private final Logger log = Logger.getLogger(CountIpBytesReduce.class);

	private Text averageAndSum = new Text();;

	private IntWritable ipBytes = new IntWritable();;

	private DoubleWritable average = new DoubleWritable();;

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		int count = 0;

		for (IntWritable value : values) {
			sum += value.get();
			count++;
		}

		average.set(sum / (double) count);
		ipBytes.set(sum);
		averageAndSum.set(String.valueOf(average) + ","
				+ String.valueOf(ipBytes));
		context.write(key, averageAndSum);
	}
}
