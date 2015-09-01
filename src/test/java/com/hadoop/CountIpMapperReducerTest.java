package com.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.hadoop.CountIpBytesMapper;
import com.hadoop.CountIpBytesReduce;

public class CountIpMapperReducerTest {

	private static final String LOG_EXAMPLE_FIRST = "ip1 - - [24/Apr/2011:04:32:09 -0400] 'GET /vanagon/VanagonProTraining/DigiFant/040.jpg HTTP/1.1' 200 118873 '-' 'Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)'";

	private static final String LOG_EXAMPLE_SECOND = "ip1 - - [24/Apr/2011:04:39:47 -0400] 'GET /sun_ss5/ss5_jumpers.jpg HTTP/1.1' 206 37295 'http://host2/sun_ss5/' 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0'";

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

	private ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;

	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		CountIpBytesMapper mapper = new CountIpBytesMapper();
		CountIpBytesReduce reducer = new CountIpBytesReduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void mapperTest() throws IOException {
		mapDriver.withInput(new LongWritable(0), new Text(LOG_EXAMPLE_FIRST))
				.withOutput(new Text("ip1"), new IntWritable(118873));
		mapDriver.withInput(new LongWritable(0), new Text(LOG_EXAMPLE_SECOND))
				.withOutput(new Text("ip1"), new IntWritable(37295));
		mapDriver.runTest();
	}

	@Test
	public void reducerTest() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(37295));
		values.add(new IntWritable(118873));

		reduceDriver.withInput(new Text("ip1"), values).withOutput(
				new Text("ip1"), new Text("78084.0,156168")); // 118873 + 37295
																// sum / 2 =
																// 78084.0
		reduceDriver.runTest();
	}

	@Test
	public void mapReduceTest() throws IOException {
		mapReduceDriver.withInput(new LongWritable(0),
				new Text(LOG_EXAMPLE_FIRST)).withOutput(new Text("ip1"),
				new Text("118873.0,118873")); // valid cuz only one input line
		mapReduceDriver.runTest();
	}
}
