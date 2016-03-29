import java.io.IOException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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
 * Ce job illustre la réponse à la question 3.2.
 * L'objectif est de créer un job qui permet de calculer le nombre de tags sans risque d'explosions mémoire du reducer, mais en une seul passe.
 *
 * Ceci est réalisable si on arrive à controler l'odre des tags dans les valeurs en entrée du reducer.
 * Ceci permet de compter et de trier les tags sans utiliser de tableau associatif.
 */
public class Question3_2 {
	
	private static final int LONGITUDE = 10;
	private static final int LATITUDE = 11;
	private static final int TAGS = 8;
	private static Counter skippedCountries;
	private enum Compteur {SKIPPED_COUNTRIES}
	
	public static class MyMapper extends Mapper<LongWritable, Text, PairStringOneShot, StringAndInt> {
				
		@Override
		protected void cleanup(Mapper<LongWritable, Text, PairStringOneShot, StringAndInt>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("MAp end");
			super.cleanup(context);
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, PairStringOneShot, StringAndInt>.Context context)
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
					context.write(new PairStringOneShot(country.toString(), tag), new StringAndInt(1 , tag));
				}	
			}
		}
	}

    // Les données dans le combiner arrivent par clé différentes de pays et tags. On peut donc commencer à compter.
    public static class Combiner extends Reducer<PairStringOneShot, StringAndInt, PairStringOneShot, StringAndInt> {

        @Override
        protected void reduce(PairStringOneShot key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (StringAndInt value : values) {
                sum += value.getIntVal();

            }
            context.write(key, new StringAndInt(sum, key.getSecond()));
        }

    }

	public static class MyReducer extends Reducer<PairStringOneShot, StringAndInt, Text, Text> {
		
		private Integer K; 		
		
		@Override
		protected void setup(Reducer<PairStringOneShot, StringAndInt, Text, Text>.Context context) throws IOException, InterruptedException {
			super.setup(context);
			K = context.getConfiguration().getInt("K", 10);
			System.out.println("Red begin");
		}
		
		@Override
		protected void reduce(PairStringOneShot key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			PriorityQueue<StringAndInt> heap = new PriorityQueue<StringAndInt>(K);
			String currentTag = null;
			StringAndInt current = null;

			for(StringAndInt val : values) {
				System.out.println(val.getStringVal());
				if( current == null | !val.getStringVal().equals(currentTag)) {
					if(current != null) {
						heap.add(current);
						if (heap.size() > K) heap.poll();
					}
					current = new StringAndInt(val.getIntVal(), val.getStringVal().toString());
					currentTag = val.getStringVal().toString();
				} else {
					current.setIntVal(current.getIntVal() + val.getIntVal());
				}
			}

			PriorityQueue<StringAndInt> heap2 = new PriorityQueue<StringAndInt>(K, Collections.reverseOrder());
			heap2.addAll(heap);
			StringBuilder sb = new StringBuilder();

			while(!heap2.isEmpty()) {
				StringAndInt value = heap2.poll();
				sb.append(value.getStringVal()).append('#').append(value.getIntVal());
				sb.append(", ");
			}
			context.write(new Text(key.getFirst()), new Text(sb.toString()));

		}
	}

	public static class NaturalKeyGroupingComparator extends WritableComparator {

		public NaturalKeyGroupingComparator() {
			super(PairStringOneShot.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			System.out.println("compare!");
			PairStringOneShot k1 = (PairStringOneShot)w1;
			PairStringOneShot k2 = (PairStringOneShot)w2;

			return k1.getFirst().compareTo(k2.getFirst());
		}

	}

	public static class CompositeKeyComparator extends WritableComparator {

		public CompositeKeyComparator() {
			super(PairStringOneShot.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			System.out.println("Sort!");
			PairStringOneShot k1 = (PairStringOneShot)w1;
			PairStringOneShot k2 = (PairStringOneShot)w2;
			int res = k1.getFirst().compareTo(k2.getFirst());
			if (res == 0) return k1.getSecond().compareTo(k2.getSecond());
			return res;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("K", 1000);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		
		Job job = Job.getInstance(conf, "Question3_2");
		job.setJarByClass(Question3_2.class);

		// Group comparator :  Les clé serons ainsi trié par le nom de pay.
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		// Sorted : Comme ca les clés arrivent dans le reducer dans le bon ordre : Pay différent et chaques tags dans l'ordre alphabétique.
		job.setSortComparatorClass(CompositeKeyComparator.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(PairStringOneShot.class);
		job.setMapOutputValueClass(StringAndInt.class);

        job.setCombinerClass(Combiner.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
