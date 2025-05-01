import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/*
 * Step1.java
 * General Purpose: Process unigrams and calculate word frequencies
 * 
 * Input: Google Books 1-gram dataset
 * Format: tab-separated values containing: ngram TAB year TAB occurrences TAB pages TAB books
 * Example: "word 1990 157 123 89"
 * 
 * Output: Word counts and corpus statistics
 * Format: word TAB count
 * Example: "word    157" or "*    1000000"
 */
public class Step1 {

    /*
     * MapperClass
     * Input:
     *   Key: LongWritable (line offset in input file)
     *   Value: Text (line from input file)
     *      Format: "word year occurrences pages books"
     *      Example: "dog 1990 157 123 89"
     * 
     * Output:
     *   Key: Text 
     *      Either word (for word counts) or "*" (for total corpus size)
     *      Example: "dog" or "*"
     *   Value: IntWritable
     *      Number of occurrences
     *      Example: 157
     * 
     * Processing:
     * 1. Splits input line into fields
     * 2. Extracts word and occurrences
     * 3. Filters non-Hebrew words and stop words
     * 4. Emits both word count and contribution to total corpus size
     */
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static Text C0_KEY = new Text("*");
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    Path path = new Path(cacheFile.toString());
                    FileSystem fs = path.getFileSystem(context.getConfiguration());
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            stopWords.add(line.trim().toLowerCase());
                        }
                    }
                }
            }
        }

       
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length >= 4) {
                String ngram = fields[0];
                int occurrences = Integer.parseInt(fields[2]);
                String[] words = ngram.split("\\s+");
                for (String w : words) {
                    w = w.trim().toLowerCase();
                    if (!w.isEmpty() && !stopWords.contains(w) && isHebrewString(w)) {
                        word.set(w);
                        context.write(word, new IntWritable(occurrences));
                        context.write(C0_KEY, new IntWritable(occurrences)); // Emit consistent C0 key
                    }
                }
            }
        }
        public static boolean isHebrewString(String str) {
            // Hebrew letters Unicode range: 0x0590 to 0x05FF
            return str.matches("^[\u0590-\u05FF]+$");
        }
    }

     /*
     * ReducerClass
     * Input:
     *   Key: Text (word or "*")
     *   Value: Iterable<IntWritable> (list of occurrence counts)
     *      Example: For word "dog": [157, 123, 89]
     *      Example: For "*": [157, 123, 89, ...]
     * 
     * Output:
     *   Key: Text (word or "*")
     *   Value: IntWritable (total count)
     *      Example: "dog    369"
     *      Example: "*    1000000"
     * 
     * Processing:
     * Sums up all occurrences for each word or total corpus size
     */
    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return (key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Step1 <input path> <output path> <stop words path>");
            System.exit(-1);
        }

        String inputPath = args[1];
        String outputPath = args[2];
        String stopWordsPath = args[3];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 1 - Process Unigrams");
        job.setJarByClass(Step1.class);

        
        job.setInputFormatClass(SequenceFileInputFormat.class); 
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.addCacheFile(new URI(stopWordsPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
