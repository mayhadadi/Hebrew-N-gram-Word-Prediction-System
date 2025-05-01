import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
 * Step4a.java
 * General Purpose: Aggregate statistics from previous steps for probability calculation
 * 
 * Input: Multiple sources -
 * 1. Step1 output (word counts)
 * 2. Step2 output (bigram counts)
 * 3. Step3 output (trigram counts)
 * 
 * Output: Combined statistics for trigram probability calculation
 * Format: "trigram TAB statistics"
 * Example: "היה היה היה    10 היה היה 100"
 */
public class Step4a {
	/*
     * Map Class
     * Input:
     *   Key: LongWritable (line offset)
     *   Value: Text (from previous steps)
     *      Format varies by input source:
     *      - From Step1: "word    count"
     *      - From Step2: "word1 word2    count"
     *      - From Step3: "word1 word2 word3    count"
     * 
     * Output:
     *   Key: Text
     *      Various formats:
     *      - "word1 word2" for bigram stats
     *      - "word2 word3" for bigram stats
     *      - Single words for unigram stats
     *   Value: Text
     *      Format: varies based on n-gram type
     *      Examples:
     *      - "word1 word2 word3 count" for trigrams
     *      - "count" for unigrams/bigrams
     * 
     * Processing:
     * 1. Identifies input source
     * 2. Extracts relevant word combinations
     * 3. Emits appropriate key-value pairs for aggregation
     */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] keyVal = value.toString().split("\t");

			String[] words = keyVal[0].split(" ");
			String w1 = words[0];
			String occ = keyVal[1];
			Text occurs = new Text(occ);
			if (w1.equals("*")) {
				context.write(new Text("*"), occurs);
				return;
			}
			Text firstWord = new Text(String.format("%s", w1));

			if (words.length > 1) {
				String w2 = words[1];
				Text firstTwo = new Text(String.format("%s %s", w1, w2));
				if (words.length > 2) {
					String w3 = words[2];
					Text newVal = new Text(String.format("%s %s %s %s", w1, w2, w3, occ));
					Text lastTwo = new Text(String.format("%s %s", w2, w3));
					Text secondWord = new Text(String.format("%s", w2));
					Text thirdWord = new Text(String.format("%s", w3));

					context.write(firstTwo, newVal);
					context.write(lastTwo, newVal);
					context.write(firstWord, newVal);
					context.write(secondWord, newVal);
					context.write(thirdWord, newVal);

				} else{
					context.write(firstTwo, occurs);}
			} else{
				context.write(firstWord, occurs);}

		}
	}

	/*
     * Reduce Class
     * Input:
     *   Key: Text (word combination)
     *   Value: Iterable<Text> (statistics)
     *      Contains various statistics needed for probability calculation
     * 
     * Output:
     *   Key: Text (trigram)
     *   Value: Text (combined statistics)
     *      Format: "N3 w1 w2 C2" or "N3 w2 w3 N2"
     *      Where:
     *      - N3: trigram count
     *      - C2: bigram count for (w1,w2)
     *      - N2: bigram count for (w2,w3)
     * 
     * Processing:
     * 1. Collects all statistics for each word combination
     * 2. Combines relevant counts and frequencies
     * 3. Prepares data for probability calculation
     */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// We wish to save a local ArrayList<String> that will hold the values received
			// in the reduce procedure,
			// until we find the amount of occurrences of "w1 w2" in the corpus.
			// After finding, we send all the key-values related to the data saved in the
			// local memory, and free that memory.
			// All next values, will not be saved in the local memory, but rather directly
			// sent to the context.
			List<String> savedVals = new ArrayList<>();

			String[] words = key.toString().split(" ");
			String w1 = words[0];
			if (w1.equals("*")) {
				String val="";
				for (Text value : values) {
					val = value.toString();
					break;
				}
				context.write(new Text("*"), new Text(val));

			} else {
			
			Text newKey = new Text();
			Text newVal = new Text();
			boolean found = false;
			int occ2 = 0; // The number of occurrences of "w1 w2" or "w1" in the corpus.
			String w2 = "**********";
			if (words.length > 1) {
				w2 = words[1];
			}

			for (Text val : values) {

				String[] v = val.toString().split(" ");
				// If we don't have the amount of occurrences of w1, w2 --> We save the value in
				// our list (ArrayList)
				// and keep looking.
				if (!found) {
					if (v.length == 1) {
						// 1. save the amount of occurrences in a "global" variable.
						occ2 = Integer.parseInt(v[0]);

						// 2. Perform the writing of all the values in our list to the context.
						for (String threes : savedVals) {
							String[] vs = threes.split(" ");

							newKey = new Text(String.format("%s %s %s", vs[0], vs[1], vs[2]));
							// New value: occ3, w1, w2, occ2
							if (w2 == "**********") {
								newVal = new Text(String.format("%s %s %d", vs[3], w1, occ2));
							} else {
								newVal = new Text(String.format("%s %s %s %d", vs[3], w1, w2, occ2));
							}
							context.write(newKey, newVal);

						}

						// 3. Clean the data-structure (list).
						savedVals.clear();

						found = true;
					} else {
						// We save the whole string, containing w1, w2, w3, the amount of their
						// occurrences - occ3
						// delimited by a space.
						savedVals.add(val.toString());
					}
				}
				// We have the amount of occurrences of the two words w1, w2 --> Can just send
				// the desired <key, value>
				// to the context.
				else {
					// Just send to context <"w1 w2 w3", "occ3 w1 w2 occ_2">
					String[] vs = val.toString().split(" ");
					newKey = new Text(String.format("%s %s %s", vs[0], vs[1], vs[2]));
					// New value: occ3, w1, w2, occ2
					if (w2 == "**********") {
						newVal = new Text(String.format("%s %s %d", vs[3], w1, occ2));
					} else {
						newVal = new Text(String.format("%s %s %s %d", vs[3], w1, w2, occ2));
					}
					context.write(newKey, newVal);
				}
			}
		}
	}
}

private static class myPartitioner extends Partitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		return Math.abs(key.hashCode()) % numPartitions;
	}

	}

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.err.println("Usage: Step4a <input1 path> <input2 path> <input3 path> <output path>");
			System.exit(-1);
		}
		String step1_path = args[1];
		String step2_path = args[2];
		String step3_path = args[3];

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Aggregate 2 and 3");
		job.setJarByClass(Step4a.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(myPartitioner.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		System.out.println(step1_path);
		MultipleInputs.addInputPath(job, new Path(step1_path), TextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(step2_path), TextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(step3_path), TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		job.waitForCompletion(true);
	}

}
