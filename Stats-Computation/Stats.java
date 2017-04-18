import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.w3c.dom.Text;

public class Stats {

	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {

		// private final static IntWritable one = new IntWritable(1); // type of
		// output
		// value
		// private DoubleWritable value = new DoubleWritable(); // type of
		// output key

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString()); // line
																			// to
																			// string
																			// token

			double number = Double.parseDouble(itr.nextToken());
			/**
			 * while (itr.hasMoreTokens()) { word.set(itr.nextToken()); // set
			 * word as each input keyword context.write("A", word); // create a
			 * pair <keyword, 1> }
			 **/
			//System.out.println("Inside Mapper - Value " + number);
			context.write(new Text("Value"), new Text(String.valueOf(number)));
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, Text> {

		// private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double final_min = 10000; // initialize the sum for each keyword
			double final_max = 0;
			double avg = 0;
			double std_dev = 0;
			double final_sum_square = 0;
			double final_sum = 0;
			double variance = 0;
			
			int final_count = 0;
			
			Iterator<Text> itr = values.iterator();
			while (itr.hasNext()) {
				String input = itr.next().toString();
				//System.out.println("Inside Reducer - Values " + input);
				String[] elements = input.split("_");

				if (final_min > Double.parseDouble(elements[0])) {
					final_min = Double.parseDouble(elements[0]);
				}
				if (final_max < Double.parseDouble(elements[1])) {
					final_max = Double.parseDouble(elements[1]);
				}
				final_count = final_count + Integer.parseInt(elements[3]);
				final_sum += Double.parseDouble(elements[2]);
				final_sum_square += Double.parseDouble(elements[4]);
			}
			// result.set(sum);
			avg = final_sum / final_count;
			variance = (final_sum_square - (final_sum * final_sum)/final_count)/(final_count - 1);
			std_dev = Math.sqrt(variance);
			
			context.write(new Text("Minimum"), new Text(String.valueOf(final_min)));
			context.write(new Text("Maximum"), new Text(String.valueOf(final_max)));
			context.write(new Text("Average"), new Text(String.valueOf(avg)));
			context.write(new Text("Standard Deviation"), new Text(String.valueOf(std_dev)));
			// create a
																		// pair
																		// <keyword,
																		// number
																		// of
			// occurences>
		}
	}

	public static class Combiner extends
			Reducer<Text, Text, Text, Text> { // private
														// Statistics
		private double min = 10000;
		private double max = 0;
		private double sum = 0;
		private double sum_square = 0;
		private int count = 0;

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			Iterator<Text> itr = values.iterator();
			
			while (itr.hasNext()) {
				String value = itr.next().toString();
				double val = Double.parseDouble(value);
				
				if (min > val) {
					min = val;
				}
				if (max < val) {
					max = val;
				}
				count = count + 1;
				sum = sum + val;
				sum_square = sum_square + (val * val);
			}
			String partialResult = String.valueOf(min) + "_" + String.valueOf(max) + "_" + String.valueOf(sum) + "_"
					+ String.valueOf(count) + "_" + String.valueOf(sum_square);
			//System.out.println("Inside Combiner - Values " + partialResult);
			context.write(new Text("Min_Max_Sum_Count_SumSquare"), new Text(partialResult));
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs(); // get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		// create a job with name "wordcount"
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(Stats.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);

		// Add a combiner here, not required to successfully run the wordcount
		// program

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
