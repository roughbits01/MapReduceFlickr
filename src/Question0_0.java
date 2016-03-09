
import java.io.IOException;
import java.net.URLDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
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
				System.out.println(longitude + "-->" + tags);
				System.out.println(country + "-->" + tags);
				for(String tag: tags.split(",")) {
					context.write(new Text(country.toString()), new Text(tag));
				}	
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String sum = "";
			for (Text value : values) {
				sum += value.toString() + ",";
			}
			context.write(key, new Text(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		
		Job job = Job.getInstance(conf, "Question0_0");
		job.setJarByClass(Question0_0.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}