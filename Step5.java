import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * Step5.java
 * General Purpose: Sort and format final results
 * 
 * Input: Step4b output (trigrams with probabilities)
 * Format: "trigram TAB probability"
 * 
 * Output: Sorted trigrams by probability (descending)
 * Format: "trigram TAB probability"
 * Example: "היה היה היה    0.0234"
 */
public class Step5 {

    public static class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
        private Text w1w2; // Prefix of trigram (w1 + " " + w2)
        private Text w3;   // The third word (w3)
        private DoubleWritable probability;

        public CompositeKeyWritable() {
            this.w1w2 = new Text();
            this.w3 = new Text();
            this.probability = new DoubleWritable();
        }

        public CompositeKeyWritable(String w1w2, String w3, double probability) {
            this.w1w2 = new Text(w1w2);
            this.w3 = new Text(w3);
            this.probability = new DoubleWritable(probability);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            w1w2.write(out);
            w3.write(out);
            probability.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            w1w2.readFields(in);
            w3.readFields(in);
            probability.readFields(in);
        }

        @Override
        public int compareTo(CompositeKeyWritable o) {
            int result = this.w1w2.compareTo(o.w1w2);
            if (result == 0) {
                result = -this.probability.compareTo(o.probability); // Descending order
            }
            return result;
        }

        public Text getW1W2() {
            return w1w2;
        }

        public Text getW3() {
            return w3;
        }

        public DoubleWritable getProbability() {
            return probability;
        }
    }

     /*
     * MapperClass
     * Input:
     *   Key: Object (line offset)
     *   Value: Text (trigram and probability)
     *      Format: "w1 w2 w3    prob"
     * 
     * Output:
     *   Key: CompositeKeyWritable
     *      Contains: w1w2, w3, probability
     *   Value: NullWritable
     * 
     * Processing:
     * 1. Parses trigram and probability
     * 2. Creates composite key for sorting
     * 3. Emits with null value
     */
    public static class MapperClass extends Mapper<Object, Text, CompositeKeyWritable, NullWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) {
                try {
                    String trigram = fields[0];
                    double probability = Double.parseDouble(fields[1]);

                    String[] words = trigram.split(" ");
                    if (words.length == 3) {
                        String w1w2 = words[0] + " " + words[1];
                        String w3 = words[2];
                        context.write(new CompositeKeyWritable(w1w2, w3, probability), NullWritable.get());
                    }
                } catch (NumberFormatException e) {
                    // Log and skip malformed probabilities
                    context.getCounter("Step5", "MalformedProbabilities").increment(1);
                }
            } else {
                context.getCounter("Step5", "MalformedRecords").increment(1);
            }
        }
    }

    /*
     * ReducerClass
     * Input:
     *   Key: CompositeKeyWritable (sorted composite key)
     *   Value: NullWritable
     * 
     * Output:
     *   Key: Text (complete trigram)
     *   Value: DoubleWritable (probability)
     *      Format: "w1 w2 w3    probability"
     *      Example: "היה היה היה    0.0234"
     * 
     * Processing:
     * 1. Extracts trigram components from composite key
     * 2. Formats output in final form
     * 3. Writes sorted results
     */
    public static class ReducerClass extends Reducer<CompositeKeyWritable, NullWritable, Text, DoubleWritable> {
        @Override
        public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(new Text(key.getW1W2() + " " + key.getW3()), key.getProbability());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Step5 <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 5 - Sort Trigram Probabilities");
        job.setJarByClass(Step5.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
