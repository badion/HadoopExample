package com.hadoop;

import java.util.regex.Matcher;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class CountIpBytesMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private final Logger log = Logger.getLogger(CountIpBytesMapper.class);
	
	private static final String IP_NAME = "^([\\w]+)";

	private static final String IP_BYTES = ".[\\d]+ ([\\d]+) .";

	private Pattern ipBytesInLine = Pattern.compile(IP_BYTES);

	private Pattern firstWordInLine = Pattern.compile(IP_NAME);

	private final Text mapKey = new Text();

	private final IntWritable ipBytes = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		Matcher matchFirstWordInLine = firstWordInLine.matcher(line);
		Matcher matchIpBytesInLine = ipBytesInLine.matcher(line);

		if (matchFirstWordInLine.find()) {
			String ip = matchFirstWordInLine.group(0).trim();
			mapKey.set(ip);
		}

		if (matchIpBytesInLine.find()) {
			String ipB = matchIpBytesInLine.group(1).trim();
			ipBytes.set(Integer.parseInt(ipB));
		}

		context.write(mapKey, ipBytes);
	}
}
