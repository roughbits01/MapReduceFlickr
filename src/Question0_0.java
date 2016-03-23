
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question0_0 {
	
	private static final int LONGITUDE = 10;
	private static final int LATITUDE = 11;
	private static final int TAGS = 8;
	private static Counter skippedCountries;
	private enum Compteur {SKIPPED_COUNTRIES};
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
				
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, StringAndInt>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, StringAndInt>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			skippedCountries = context.getCounter(Compteur.SKIPPED_COUNTRIES);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] data = value.toString().split("\\t");
			double longitude = Double.parseDouble(data[LONGITUDE]);
			double latitude = Double.parseDouble(data[LATITUDE]);
			Country country = Country.getCountryAt(latitude, longitude);
			
			if (country == null) {
				skippedCountries.increment(1);
			} else {
				String tags = URLDecoder.decode(data[TAGS], "utf-8");
				for(String tag: tags.split(",")) {
					if (tag.replace("\\s+", "").isEmpty()) continue;
					context.write(new Text(country.toString()), new StringAndInt(1, tag));
				}	
			}
		}
	}

	public static class MyReducer extends Reducer<Text, StringAndInt, Text, Text> {
		
		private Integer K; 		
		
		@Override
		protected void setup(Reducer<Text, StringAndInt, Text, Text>.Context context) throws IOException, InterruptedException {
			super.setup(context);
			K = context.getConfiguration().getInt("K", 10);
		}
		
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {

			Map<String, StringAndInt> counter = new HashMap<String, StringAndInt>(); 
			
			for (StringAndInt value : values) {
				StringAndInt count = counter.get(value.getStringVal());
				if (count == null) {
					counter.put(value.getStringVal(), new StringAndInt(value.getIntVal(), value.getStringVal()));
				} else {
					count.setIntVal(count.getIntVal() + value.getIntVal());
				}
			}
			
			Collection<StringAndInt> vals = counter.values();
			PriorityQueue<StringAndInt> heap = new PriorityQueue<StringAndInt>(vals.size(), Collections.reverseOrder());
			heap.addAll(vals);

			StringBuilder sb = new StringBuilder();
			Integer count = 1;
			while(!heap.isEmpty()) {
				StringAndInt element = heap.poll();				
				sb.append(element.getStringVal());
		 	    if (count.equals(K) || heap.isEmpty()) break;
		 	    count++;
		 	    sb.append(", ");
			}
	        context.write(key, new Text(sb.toString()));
		}
	}
	
public static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {	
		
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {

			Map<String, StringAndInt> counter = new HashMap<String, StringAndInt>(); 
			
			for (StringAndInt value : values) {
				StringAndInt count = counter.get(value.getStringVal());
				if (count == null) {
					counter.put(value.getStringVal(), new StringAndInt(value.getIntVal(), value.getStringVal()));
				} else {
					count.setIntVal(count.getIntVal() + value.getIntVal());
				}
			}
			
			for (StringAndInt value: counter.values()) {
				context.write(key, value);
			}     
		}
	}

	/*public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("K", 10);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		
		Job job = Job.getInstance(conf, "Question0_0");
		job.setJarByClass(Question0_0.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setCombinerClass(MyCombiner.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}*/
}