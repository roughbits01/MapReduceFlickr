import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

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
 * Version du wordcount utilisé pour compter le nombre de mots des misérables.
 * Cette version illustre la question 1.6 du in-mapper comiber
 */
public class WordCount {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text text = new Text();
        private final Map<String, Long> map = new HashMap<String, Long>();
        private Counter empty_lines;
        private enum Compteur {LIGNE_VIDE};

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().replaceAll("\\s+", "").isEmpty()) {
                empty_lines.increment(1);
            }
            for (String word : value.toString().replaceAll("[^0-9a-zA-Z éèàêî]", "").toLowerCase().split("\\s+")) {
                if (!word.isEmpty()) {
                    Long count = map.get(word);
                    if (count == null) {
                        map.put(word, new Long(1));
                    }
                    else {
                        map.put(word, count + 1);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            Iterator<Entry<String, Long>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Long> pair = it.next();
                text.set(pair.getKey());
                context.write(text, new LongWritable(pair.getValue()));
                it.remove(); // avoids a ConcurrentModificationException
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
        job.setJarByClass(WordCount.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(3);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}