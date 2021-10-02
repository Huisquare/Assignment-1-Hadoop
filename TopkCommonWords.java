// Matric Number: A0188608N
// Name: Li Huihui
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//for sequence file output and input formats
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.google.common.collect.ComparisonChain;

//imports for reading in stop words
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.StringBuilder;

//imports for partitioner
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopkCommonWords {

	// First Mapper - will output (word-text-i , 1)
	public static class TokenizerMapper1 extends Mapper<Object, Text, CompositeKey, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {		//i think context is an IntWritable

			StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
				CompositeKey cKey = new CompositeKey(word.toString(), 1);
				context.write(cKey, one);
            }
        }
    }

	// Second Mapper - will output (word-text-i , 2)
	public static class TokenizerMapper2 extends Mapper<Object, Text, CompositeKey, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				CompositeKey cKey = new CompositeKey(word.toString(), 2);
				context.write(cKey, one);
			}
		}
	}

	// Reducer for First and Second Mapper
	public static class IntSumReducer extends Reducer<CompositeKey, IntWritable, CompositeKey, IntWritable> {
        private IntWritable result = new IntWritable();

		public void reduce(CompositeKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

	/**
	 * Third Mapper Input: ((word, text_num), num_occurence) Output: ((word,
	 * text_num), num_occurence)
	 */
	public static class StopWordsMapper extends Mapper<CompositeKey, IntWritable, CompositeKey, IntWritable> {

		private Set<String> stopWords;

		protected void setUp(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			stopWords = new HashSet<String>();
			for (String word : conf.get("stopwords").split(",")) {
				stopWords.add(word);
			}
		}

		public void map(CompositeKey key, IntWritable value, Context context) throws IOException, InterruptedException {
			String word = null;
			try {
				word = key.getWord();
				if (!stopWords.contains(word)) {
					context.write(key, value);
				}
			} catch (NullPointerException e) {
				System.out.println("in 3rd map: word is NULL.");
			}
		}
	}

	public static class OccurenceReducer extends Reducer<CompositeKey, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private Text word = new Text();

		public void reduce(CompositeKey key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Integer min = Integer.MAX_VALUE;
			for (IntWritable val : values) {
				if (val.get() < min) {
					min = val.get();
				}
			}
			result.set(min);
			String w = key.getWord();
			word.set(w);
			context.write(word, result);
		}
	}

	public static class WordPartitioner extends Partitioner<CompositeKey, NullWritable> {

		@Override
		public int getPartition(CompositeKey key, NullWritable value, int numPartitions) {
			return Math.abs(key.getWord().hashCode()) % numPartitions;
		}

	}

	public static class CompositeKey implements WritableComparable<CompositeKey> {
		private String word;
		private int source; // source is either 1 or 2 (input 1 or input 2)

		public CompositeKey() {
		}

		public CompositeKey(String word, int source) {
			this.word = word;
			this.source = source;
		}

		public String getWord() {
			return this.word;
		}

		public int getSource() {
			return this.source;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(word);
			out.writeInt(source);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			word = in.readUTF();
			source = in.readInt();
		}

		@Override
		public int compareTo(CompositeKey other) {
			return ComparisonChain.start().compare(word, other.word).compare(source, other.source).result();
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof CompositeKey)) {
				System.out.println("object is not a composite key!");
				return false;
			}
			CompositeKey other = (CompositeKey) o;
			if (this.word.equals(other.getWord()) && (this.source == other.getSource())) {
				return true;
			} else {
				return false;
			}
		}

		@Override
		public String toString() {
			return "(" + word + ", " + source + ")";
		}

		@Override
		public int hashCode() { // example taken from hadoop WritableComparable API
			final int prime = 31;
			int result = 1;
			result = prime * result + word.hashCode();
			result = result + (int) (source ^ source >>> 32);
			return result;
		}
	}

	private static String readStopwords(String stopwordsPath) {
		StringBuilder sb = new StringBuilder();
		String wordList = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(stopwordsPath));
			String word = br.readLine();

			while (word != null) {
				sb.append(word).append(",");
				word = br.readLine();
			}
			wordList = sb.toString();
			br.close();
		} catch (Exception e) {
			System.out.println("Exception at reading in stop words");
			System.exit(1);
		} finally {
			return wordList;
		}
	}
	public static void main(String[] args) throws Exception {
		/*First Mapper*/
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(TopkCommonWords.class);
		job.setMapperClass(TokenizerMapper1.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(IntWritable.class);

		/*
		 * setting outputformat to be sequence file
		 */
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.NONE); // no compression

		FileInputFormat.addInputPath(job, new Path(args[0])); // input1
		SequenceFileOutputFormat.setOutputPath(job, new Path("./commonwords/wc_output/counted_input_1"));
		job.waitForCompletion(true);


		/*Second Mapper*/
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "word count 2");
		job2.setJarByClass(TopkCommonWords.class);
		job2.setMapperClass(TokenizerMapper2.class);
		job2.setCombinerClass(IntSumReducer.class);
		job2.setReducerClass(IntSumReducer.class);

		job2.setOutputKeyClass(CompositeKey.class);
		job2.setOutputValueClass(IntWritable.class);

		/*
		 * setting outputformat to be sequence file
		 */
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job2, CompressionType.NONE); // no compression

		FileInputFormat.addInputPath(job2, new Path(args[1])); // input2
		FileOutputFormat.setOutputPath(job2, new Path("commonwords/wc_output/counted_input_2"));
		job2.waitForCompletion(true);


		/*
		 * MapReduce for removal of stopwords (map) and selection of smaller number of
		 * occurence (reduce)
		 */
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "remove and select smaller");

		/* read the stopwords into memory */
		String stopWords = readStopwords(args[2]);

		System.out.println("stop words are read, the first word is: " + stopWords.split(",")[0]);

		conf3.set("stopwords", stopWords);

		job3.setJarByClass(TopkCommonWords.class);
		job3.setMapperClass(StopWordsMapper.class);
		// job3.setCombinerClass(OccurenceReducer.class);
		job3.setReducerClass(OccurenceReducer.class);

		job3.setPartitionerClass(WordPartitioner.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);

		// take in Sequence file as input
		job3.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.addInputPaths(job3,
				"./commonwords/wc_output/counted_input_1,./commonwords/wc_output/counted_input_2");
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}

}
