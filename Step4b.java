import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;

/*
 * Step4b.java
 * General Purpose: Calculate final probabilities for trigrams
 * 
 * Input: Step4a output (combined statistics)
 * Format: "trigram TAB combined_stats"
 * 
 * Output: Trigrams with calculated probabilities
 * Format: "trigram TAB probability"
 * Example: "היה היה היה    0.0234"
 */
public class Step4b {
	/*
     * Map Class
     * Input:
     *   Key: LongWritable (line offset)
     *   Value: Text (combined statistics)
     *      Format: "w1 w2 w3    N3 w1 w2 C2"
     * 
     * Output:
     *   Key: Text (trigram)
     *   Value: Text (probability components)
     *      Format: Preserved statistics for probability calculation
     * 
     * Processing:
     * 1. Extracts trigram and statistics
     * 2. Distributes C0 value to all reducers
     * 3. Prepares data for probability calculation
     */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] keyVal = value.toString().split("\t");
			
			if (keyVal[0].equals("*")) {
				// Get number of reducers
				int numReducers = context.getNumReduceTasks();
				// Send to all reducers by adding reducer number to key
				for (int i = 0; i < numReducers; i++) {
					context.write(new Text("*_"+i), new Text(keyVal[1]));
				}
			} else {
				context.write(new Text(keyVal[0]), new Text(keyVal[1]));
			}
		}
	}

	/*
     * Reduce Class
     * Input:
     *   Key: Text (trigram)
     *   Value: Iterable<Text> (probability components)
     *      Contains all necessary values for probability calculation
     * 
     * Output:
     *   Key: Text (trigram)
     *   Value: Text (calculated probability)
     *      Format: "trigram    probability"
     *      Example: "היה היה היה    0.0234"
     * 
     * Processing:
     * 1. Extracts all necessary components
     * 2. Calculates k2 and k3 values
     * 3. Applies probability formula:
     *    prob = (k3 * (N3/C2)) + ((1-k3) * k2 * (N2/C1)) + ((1-k3) * (1-k2) * (N1/C0))
     */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public static Long C0 = 0L;

		/*
		 *                              Index of constants:
		 *                              C0: The number of words in the corpus (single
		 *                              words).
		 *                              C1: The number of appearances of w2 in the
		 *                              corpus.
		 *                              C2: The number of appearances of (w1 w2) in the
		 *                              corpus.
		 *                              N1: The number of appearances of w3 in the
		 *                              corpus.
		 *                              N2: The number of appearances of (w2 w3) in the
		 *                              corpus.
		 *                              N3: The number of appearances of "w1, w2, w3" in
		 *                              the corpus.
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			try {
				String k = key.toString();
				// Remove the prefix if it's a "*" key
				if (k.startsWith("*")) {
					k = "*";
				}
				if (k.toString() == "*") {
					String val = "";
					
					for (Text value : values) {
						val = value.toString();
						break;
					}
					C0 = Long.parseLong(val);
				}
				else{

				String[] strings = key.toString().split(" ");
				String w1 = strings[0];
				String w2 = strings[1];
				String w3 = strings[2];

				Double N3 = 1.0;
				Double N2 = 0.0;
				Double N1 = 0.0;
				Double C1 = 0.0;
				Double k2 = 0.0;
				Double k3 = 0.0;
				Double C2 = 0.0;
				Double prob = 0.0;

				for (Text val : values) {
					String[] vals = val.toString().split(" ");

					if (N3 < 0)
						N3 = Double.parseDouble(vals[0]);

					if (vals[1].equals(w1)) {
						if (vals.length > 3) {
							if (vals[2].equals(w2)) {
								C2 = Double.parseDouble(vals[3]);
							}

						}
						k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);
					} else if (vals[1].equals(w2)) {
						if (vals.length > 3) {
							if (vals[2].equals(w3)) {
								N2 = Double.parseDouble(vals[3]);
								k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);
							}

						} else {
							C1 = Double.parseDouble(vals[2]);

						}
					} else if (vals[1].equals(w3)) {

						N1 = Double.parseDouble(vals[2]);

					}

					else
						System.out.println("Something bad happend! ");
				}
				if (C2 == 0 || C1 == 0 || C0 == 0) {
					if(C0==0){prob=0.0;}
					if(C1==0){prob=0.1;}
					if(C2==0){prob=0.2;}
				} else
				prob = (k3 * (N3 / C2)) + ((1 - k3) * k2 * (N2 / C1)) + ((1 - k3) * (1 - k2) * (N1 / C0));
				Text newKey = new Text(key.toString());
				Text newVal = new Text(prob.toString());
				context.write(newKey, newVal);
			}
			} catch (Exception e) {
				System.out.println("Problem with reduce");
				e.printStackTrace();
			}
		}

	}

	private static class myPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String k = key.toString();
			// If this is a marked "*" key, extract the partition number
			if (k.startsWith("*")) {
				return Integer.parseInt(k.split("_")[1]);
			}
			// Other keys go to their normal partition
			return Math.abs(k.hashCode()) % numPartitions;
		}
	}
	public static void main(String[] args) throws Exception {
	
		if (args.length < 3) {
			System.err.println("Usage: Step4b <input path> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Probability calculation");
		job.setJarByClass(Step4b.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(myPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}
}