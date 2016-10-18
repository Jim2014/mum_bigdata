
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

public class CrystalBallPair {

	public static class PairWritable implements
			WritableComparable<PairWritable> {

		private Integer key;

		public Integer getKey() {
			return key;
		}

		public void setKey(Integer k) {
			this.key = k;
		}

		public Integer getValue() {
			return value;
		}

		public void setValue(Integer v) {
			this.value = v;
		}

		private Integer value;

		public PairWritable(Integer k, Integer v) {
			key = k;
			value = v;
		}

		public PairWritable() {
			key= 0;
			value = 0;
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(key);
			out.writeInt(value);
		}

		public void readFields(DataInput in) throws IOException {
			key = in.readInt();
			value = in.readInt();
		}

		public int compareTo(PairWritable o) {
			return ComparisonChain.start().compare(key, o.key)
					.compare(value, o.value).result();
		}

		@Override
		public String toString() {
			return String.format("(%d,%d)", key,value);
		}

		@Override
		public int hashCode() {
			Integer sum=key+value;
			return sum.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if(obj==null|| obj.getClass()!= PairWritable.class)
				return false;
			PairWritable p=(PairWritable)obj;
			return key==p.getKey()&&value==p.getValue();
		}
		

	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, PairWritable, IntWritable> {
		Map<PairWritable, Integer> map = new HashMap<PairWritable, Integer>();
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			if(itr.hasMoreTokens())
				itr.nextToken();//first token is user, removed
			List<Integer> prodList= new ArrayList<Integer>();
			while (itr.hasMoreTokens()) {
				prodList.add(Integer.parseInt(itr.nextToken()));
			}
			for(int i=0;i<prodList.size();i++){
				Integer prod=prodList.get(i);
				PairWritable pair0=new PairWritable(prod,0);  //0 is the special * means the count of relative products of prod
				if (!map.containsKey(pair0)){					
					map.put(pair0,0);
				}
				for(int j=i+1;j<prodList.size()&&prodList.get(j)!=prod;j++){
					PairWritable pair=new PairWritable(prod,prodList.get(j));
					if (!map.containsKey(pair)){
						map.put(pair, 1);
						map.put(pair0, map.get(pair0) + 1);
					}
					else{
						map.put(pair, map.get(pair) + 1);
						map.put(pair0, map.get(pair0) + 1);
					}					
				}
			}			

		}
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, PairWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for (Entry<PairWritable, Integer> e : map.entrySet()) {
				context.write(e.getKey(),new IntWritable(e.getValue()));
			}
			super.cleanup(context);
		}
		
		
	}

	public static class MyReducer extends
			Reducer<PairWritable, IntWritable, PairWritable, DoubleWritable> {

		private int marginal = 0;

		@Override
		public void reduce(PairWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
						
			if(key.value==0){
				marginal=0;
				for(IntWritable c:values){
					marginal+=c.get();
				}
			}
			else {
				for(IntWritable c:values){
					context.write(key, new DoubleWritable(1.0*c.get()/marginal));;
				}
			}
		}


	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Inverted Index");

		job.setJarByClass(CrystalBallPair.class);

		FileInputFormat.addInputPath(job, new Path("Input"));
		FileOutputFormat.setOutputPath(job, new Path("Output1"));
		

		job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		// map output types
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		// reducer output types
		job.setOutputKeyClass(PairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
