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
 * Step2.java
 * General Purpose: Process bigrams and calculate pair frequencies
 * 
 * Input: Google Books 2-gram dataset
 * Format: tab-separated values containing: ngram TAB year TAB occurrences TAB pages TAB books
 * Example: "word1 word2 1990 157 123 89"
 * 
 * Output: Bigram counts
 * Format: "word1 word2 TAB count"
 * Example: "hello world    157"
 */
public class Step2 {

     /*
     * MapperClass
     * Input:
     *   Key: LongWritable (line offset)
     *   Value: Text (line from input file)
     *      Format: "word1 word2 year occurrences pages books"
     *      Example: "hello world 1990 157 123 89"
     * 
     * Output:
     *   Key: Text (bigram)
     *      Format: "word1 word2"
     *      Example: "hello world"
     *   Value: IntWritable (occurrences)
     *      Example: 157
     * 
     * Processing:
     * 1. Splits input into words and metadata
     * 2. Filters non-Hebrew words and stop words
     * 3. Creates bigram key
     * 4. Emits bigram count
     */
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text bigram = new Text();
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
                String ngramText = fields[0].trim();
                int occurrences = Integer.parseInt(fields[2]);
                String[] words = ngramText.split("\\s+");
                if (words.length == 2) {
                    String word1 = words[0].trim().toLowerCase();
                    String word2 = words[1].trim().toLowerCase();

                    if (!word1.isEmpty() && !word2.isEmpty() &&
                            !stopWords.contains(word1) && !stopWords.contains(word2) && isHebrewString(word1 + word2)) {
                        bigram.set(word1 + " " + word2);
                        context.write(bigram, new IntWritable(occurrences));
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
     *   Key: Text (bigram)
     *   Value: Iterable<IntWritable> (occurrence counts)
     *      Example: For "hello world": [157, 123, 89]
     * 
     * Output:
     *   Key: Text (bigram)
     *   Value: IntWritable (total count)
     *      Example: "hello world    369"
     * 
     * Processing:
     * Aggregates all occurrences for each bigram
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
            System.err.println("Usage: Step2 <input path> <output path> <stop words path>");
            System.exit(-1);
        }

        String inputPath = args[1];
        String outputPath = args[2];
        String stopWordsPath = args[3];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 2 - Calculate N2");
        job.setJarByClass(Step2.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new URI(stopWordsPath));
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
