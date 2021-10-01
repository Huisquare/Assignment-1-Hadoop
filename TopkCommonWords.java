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
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {		//i think context is an IntWritable
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);  //need to change this
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
		String inputPaths = ""+ args[0] + "," + args[1] + "," + args[2];
        //FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPaths(job, inputPaths);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

public class CompositeKey implements WritableComparable<CompositeKey> {
	private String word;
	private int source; // source is either 1 or 2 (input 1 or input 2)

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
