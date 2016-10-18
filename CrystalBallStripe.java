import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.ComparisonChain;

public class CrystalBallStripe {

	public static class StripeWritable implements Writable {

		private Map<Integer, Double> data = new HashMap<Integer, Double>();

		public Map<Integer, Double> getData() {
			return data;
		}
		public void merge(Integer key,Double value){
			if(!data.containsKey(key))
				data.put(key, value);
			else
				data.put(key,data.get(key)+value);
		}
		public void clear(){
			data.clear();
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			int key = in.readInt();
			Double value = in.readDouble();
			data.put(key, value);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			for (Entry<Integer, Double> e : data.entrySet()) {
				out.writeInt(e.getKey());
				out.writeDouble(e.getValue());
			}

		}
		@Override
		public String toString() {
			StringBuilder s=new StringBuilder();
			for (Entry<Integer, Double> e : data.entrySet()) {
				s.append(String.format("(%d,%f)", e.getKey(),e.getValue()));
			}
			return s.toString();
		}
		

	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, IntWritable, StripeWritable> {
		Map<Integer,StripeWritable > map = new HashMap<Integer,StripeWritable>();

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
			for (Entry<Integer,StripeWritable> e : map.entrySet()) {
				context.write(new IntWritable(e.getKey()), e.getValue());
			}
			super.cleanup(context);
		}

	}

	public static class MyReducer
			extends
			Reducer<IntWritable,StripeWritable, IntWritable,StripeWritable> {

		 

		@Override
		public void reduce(IntWritable key, Iterable<StripeWritable> values,
				Context context) throws IOException, InterruptedException {
			StripeWritable newStripe=new StripeWritable();
			int marginal = 0;
			for(StripeWritable v:values){
				for(Entry<Integer,Double> e: v.getData().entrySet()){
					newStripe.merge(e.getKey(), e.getValue());
					marginal+=e.getValue();
				}
			}
			for(Entry<Integer,Double> e: newStripe.getData().entrySet()){
				e.setValue(e.getValue()/marginal);
			}
			context.write(key, newStripe);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Inverted Index");

		job.setJarByClass(CrystalBallStripe.class);

		FileInputFormat.addInputPath(job, new Path("Input"));
		FileOutputFormat.setOutputPath(job, new Path("Output2"));

		job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		// map output types
		job.setMapOutputKeyClass(StripeWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		// reducer output types
		job.setOutputKeyClass(StripeWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
