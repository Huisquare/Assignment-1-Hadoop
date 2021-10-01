// Matric Number: A0188608N
// Name: Li Huihui
import java.io.DataInput;
import java.io.IOException;
import java.util.StringTokenizer;

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

public class TopkCommonWords {

	// First Mapper - will output (word-text-i , 1)
	public static class TokenizerMapper1 extends Mapper<Object, Text, CompositeKey, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, org.w3c.dom.Text value, Context context) throws IOException, InterruptedException {		//i think context is an IntWritable

			StringTokenizer itr = new StringTokenizer(value.toString(), "(space)\t\n\r\f");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
				CompositeKey cKey = new CompositeKey(word, 1);
				context.write(cKey, one);
            }
        }
    }

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

	public static class TokenizerMapper2 extends Mapper<Object, Text, CompositeKey, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "(space)\t\n\r\f");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				CompositeKey cKey = new CompositeKey(word, 2);
				context.write(cKey, one);
			}
		}
	}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(TopkCommonWords.class);
		job.setMapperClass(TokenizerMapper1.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(CompositeKey.class);
        job.setOutputValueClass(IntWritable.class);

		// String inputPaths = ""+ args[0] + "," + args[1] + "," + args[2];
		FileInputFormat.addInputPath(job, new Path(args[0])); // input1
		// FileInputFormat.addInputPaths(job, inputPaths);
		FileOutputFormat.setOutputPath(job, new Path("commonwords/wc_output/counted_input_1"));
		// System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf, "word count");
		job2.setJarByClass(TopkCommonWords.class);
		job2.setMapperClass(TokenizerMapper2.class);
		job2.setCombinerClass(IntSumReducer.class);
		job2.setReducerClass(IntSumReducer.class);

		job2.setOutputKeyClass(CompositeKey.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(args[1])); // input2
		FileOutputFormat.setOutputPath(job2, new Path("commonwords/wc_output/counted_input_2"));
		job2.waitForCompletion(true);
    }
}

public class CompositeKey implements WritableComparable<CompositeKey> {
	private String word;
	private int source; // source is either 1 or 2 (input 1 or input 2)

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
		out.writeString(word);
		out.writeInt(source);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word = in.readLine();
		source = in.readInt();
	}

	@Override
	public int compareTo(CompositeKey other) {
		return ComparisonChain.start().compare(word, other.word).compare(source, other.source).result();
	}

	@Override
	public boolean equals(CompositeKey other) {
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
