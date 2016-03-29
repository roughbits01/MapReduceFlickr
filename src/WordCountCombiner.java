
import java.io.IOException;

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

/**
 * Version du wordcoun classique avec un combiner entre le mapper et le reducer.
 */
public class WordCountCombiner {
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private final Text text = new Text();
		private final static LongWritable ONE = new LongWritable(1);
		private Counter empty_lines;
		private enum Compteur {LIGNE_VIDE};

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (value.toString().replaceAll("\\s+", "").isEmpty()) {
				empty_lines.increment(1);
				return;
			}
			for (String word : value.toString().replaceAll("[^0-9a-zA-Z éèàêî]", "").toLowerCase().split("\\s+")) {
				if (!word.isEmpty()) {
					text.set(word);
					context.write(text, ONE);
				}
			}

		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			empty_lines = context.getCounter(Compteur.LIGNE_VIDE);
		}
	}

	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable longwrit = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			longwrit.set(sum);
			context.write(key, longwrit);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountCombiner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


