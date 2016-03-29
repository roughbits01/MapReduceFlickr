
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Class effectuant un job mapReduce en réponse à la question 3.1.
 * Ce job composé permet un meilleur contrôle sur la taille de la mémoire utilisé (lors du reducer).
 * En effet, via un premier job on compte le nombre d'occurence d'un tag par pays et un second job récuperera ainsi les K meilleurs.
 */
public class Question3_1 {
	
	private static final int LONGITUDE = 10;
	private static final int LATITUDE = 11;
	private static final int TAGS = 8;

	public static class MapperJob1 extends Mapper<LongWritable, Text, PairString, LongWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] data = value.toString().split("\\t"); // Séparateurs des champs.
			double longitude = Double.parseDouble(data[LONGITUDE]);
			double latitude = Double.parseDouble(data[LATITUDE]);
			Country country = Country.getCountryAt(latitude, longitude); // Récupération du pays associé.
			
			if (country != null) {
				String tags = URLDecoder.decode(data[TAGS], "utf-8");
				for(String tag: tags.split(",")) {
					if (tag.replace("\\s+", "").isEmpty()) continue;
					context.write(new PairString( country.toString(), tag),new LongWritable(1));
				}	
			}
		}
	}

	// Ce reducer permet un première aggregation local des occurences de tags.
	public static class CombinerJob1 extends Reducer<PairString, LongWritable, PairString, LongWritable> {

		@Override
		protected void reduce(PairString key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}


	public static class ReducerJob1 extends Reducer<PairString, LongWritable, PairString, LongWritable> {

		@Override
		protected void reduce(PairString key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

			Long sum = 0l;
			
			for (LongWritable value : values) {
				sum += value.get();
			}
			
	        context.write(key, new LongWritable(sum));
		}
	}
	
	public static class MapperJob2 extends Mapper<PairString, LongWritable, Text, StringAndInt> {

		@Override
		protected void map(PairString key, LongWritable value, Context context) throws IOException, InterruptedException {
			context.write(new Text(key.getFirst()), new StringAndInt(new Long(value.get()).intValue(), key.getSecond()));
		}
	}

	public static class ReducerJob2 extends Reducer<Text, StringAndInt, Text, Text> {
		
		private Integer K;
		
		@Override
		protected void setup(Reducer<Text, StringAndInt, Text, Text>.Context context) throws IOException, InterruptedException {
			super.setup(context);
			K = context.getConfiguration().getInt("K", 5);
		}

		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			
			PriorityQueue<StringAndInt> heap = new PriorityQueue<StringAndInt>(K);
			
			for (StringAndInt value: values) {
				heap.add(new StringAndInt(value.getIntVal(), value.getStringVal().toString()));
				if (heap.size() > K) heap.poll();	
			}

			StringBuilder sb = new StringBuilder();
			
			PriorityQueue<StringAndInt> heap2 = new PriorityQueue<StringAndInt>(K, Collections.reverseOrder());
			heap2.addAll(heap);
			
			while(!heap2.isEmpty()) {
				StringAndInt value = heap2.poll();
				sb.append(value.getStringVal()).append('#').append(value.getIntVal());
		 	    sb.append(", ");
			}
	        context.write(key, new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("K", 5);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String finalOutput = otherArgs[2];
		
		Job job1 = Job.getInstance(conf, "Question3_1");
		job1.setJarByClass(Question3_1.class);
		
		job1.setMapperClass(MapperJob1.class);
		job1.setMapOutputKeyClass(PairString.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setReducerClass(ReducerJob1.class);
		job1.setOutputKeyClass(PairString.class);
		job1.setOutputValueClass(LongWritable.class);

		job1.setCombinerClass(CombinerJob1.class);

		FileInputFormat.addInputPath(job1, new Path(input));
		job1.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job1, new Path(output));
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
				
		if(job1.waitForCompletion(true)) { // si le premier job s'est terminé avec succès on lance le second.
			Job job2 = Job.getInstance(conf, "Question3_1");
			job2.setJarByClass(Question3_1.class);
			
			job2.setMapperClass(MapperJob2.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(StringAndInt.class);

			job2.setReducerClass(ReducerJob2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job2, new Path(output));
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			
			FileOutputFormat.setOutputPath(job2, new Path(finalOutput));
			job2.setOutputFormatClass(TextOutputFormat.class);
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		} else {
			System.exit(1);
		}
	}
}
