import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.MapWritable;

public class CrystalBallStripe {

	public static class StripeWritable extends MapWritable {
		public void merge(Integer key, Double value) {
			IntWritable k = new IntWritable(key);
			DoubleWritable v = new DoubleWritable(value);
			if (!this.containsKey(k))
				this.put(k, v);
			else
				this.put(k,
						new DoubleWritable(((DoubleWritable) this.get(k)).get()
								+ value));
		}

		public void merge(Writable key, Writable value) {
			IntWritable k = (IntWritable) (key);
			DoubleWritable v = (DoubleWritable) (value);
			if (!this.containsKey(k))
				this.put(k, v);
			else
				this.put(k,
						new DoubleWritable(((DoubleWritable) this.get(k)).get()
								+ v.get()));
		}

		@Override
		public String toString() {
			StringBuilder s = new StringBuilder();
			for (Entry<Writable, Writable> e : this.entrySet()) {
				s.append(String.format("(%d,%f)",
						((IntWritable) e.getKey()).get(),
						((DoubleWritable) e.getValue()).get()));
			}
			return s.toString();
		}

	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, IntWritable, StripeWritable> {
		Map<Integer, StripeWritable> map = new HashMap<Integer, StripeWritable>();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());

			if (itr.hasMoreTokens())
				itr.nextToken();// first token is user, removed
			List<Integer> prodList = new ArrayList<Integer>();
			while (itr.hasMoreTokens()) {
				prodList.add(Integer.parseInt(itr.nextToken()));
			}
			for (int i = 0; i < prodList.size(); i++) {
				Integer prod = prodList.get(i);

				if (!map.containsKey(prod)) {
					map.put(prod, new StripeWritable());
				}
				for (int j = i + 1; j < prodList.size()
						&& prodList.get(j) != prod; j++) {
					map.get(prod).merge(prodList.get(j), 1.0);
				}
			}

		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, IntWritable, StripeWritable>.Context context)
				throws IOException, InterruptedException {
			for (Entry<Integer, StripeWritable> e : map.entrySet()) {
				if(e.getValue().size()>0)
					context.write(new IntWritable(e.getKey()), e.getValue());
			}
			super.cleanup(context);
		}

	}

	public static class MyReducer extends
			Reducer<IntWritable, StripeWritable, IntWritable, StripeWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<StripeWritable> values,
				Context context) throws IOException, InterruptedException {
			StripeWritable newStripe = new StripeWritable();
			Double marginal = 0.0;
			System.out.printf("reduce: key=%d \r\n", key.get());
			for (StripeWritable v : values) {
				System.out.println("new value");
				for (Entry<Writable, Writable> e : v.entrySet()) {
					newStripe.merge(e.getKey(), e.getValue());
					marginal += ((DoubleWritable) e.getValue()).get();
				}
			}
			for (Entry<Writable, Writable> e : newStripe.entrySet()) {
				e.setValue(new DoubleWritable(((DoubleWritable) e.getValue())
						.get() / marginal));
			}
			context.write(key, newStripe);
		}

	}

	public static class MyPartitioner extends
			Partitioner<IntWritable, StripeWritable> {

		@Override
		public int getPartition(IntWritable key, StripeWritable value,
				int numReducer) {
			return (key.hashCode() & Integer.MAX_VALUE) % numReducer;
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CrystalBallStripe");

		job.setJarByClass(CrystalBallStripe.class);

		FileInputFormat.addInputPath(job, new Path("Input"));
		FileOutputFormat.setOutputPath(job, new Path("Output2"));

		job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		job.setPartitionerClass(MyPartitioner.class);
		// map output types
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(StripeWritable.class);
		// reducer output types
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(StripeWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
