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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.ComparisonChain;

public class CrystalBallHybird {

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

	public static class MyPartitioner extends Partitioner<PairWritable, IntWritable>{

		@Override
		public int getPartition(PairWritable key, IntWritable value, int numReducer) {
			
			return (key.getKey().hashCode()&Integer.MAX_VALUE)%numReducer;
		
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

				for(int j=i+1;j<prodList.size()&&prodList.get(j)!=prod;j++){
					PairWritable pair=new PairWritable(prod,prodList.get(j));
					if (!map.containsKey(pair)){
						map.put(pair, 1);					
					}
					else{
						map.put(pair, map.get(pair) + 1);				
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
				System.out.printf("map o/p: (%d,%d)-%d",e.getKey().getKey(),e.getKey().getValue(),e.getValue());
			}
			
			super.cleanup(context);
		}
		
		
	}

	public static class MyReducer extends
			Reducer<PairWritable, IntWritable, IntWritable, StripeWritable> {

		private double marginal = 0;
		private Integer curTerm = null;
		StripeWritable stripe= new StripeWritable();
		
		@Override
		public void reduce(PairWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			if(curTerm==null){
				curTerm=new Integer(key.getKey());
			}
			else if(!curTerm.equals(key.getKey())){
				for(Entry<Integer,Double> e:stripe.getData().entrySet()){
					e.setValue(e.getValue()/marginal);
				}
				context.write(new IntWritable(curTerm),stripe);
				stripe.clear();
				marginal=0.0;
				curTerm=key.getKey();
			}
			for(IntWritable v:values){
				marginal+=(double)v.get();
				stripe.merge(key.getValue(), (double)v.get());
			}
		}

		@Override
		protected void cleanup(
				Reducer<PairWritable, IntWritable, IntWritable, StripeWritable>.Context context)
				throws IOException, InterruptedException {
			for(Entry<Integer,Double> e:stripe.getData().entrySet()){
				e.setValue(e.getValue()/marginal);
			}
			context.write(new IntWritable(curTerm),stripe);
			super.cleanup(context);
		}
		


	}

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
			int size=in.readInt();
			int i=size;
			while((i--)>0){
				int key = in.readInt();
				Double value = in.readDouble();
				data.put(key, value);	
				System.out.printf("Read:size=%d,key=%d,v=%f\r\n",size,key,value);
			}
			
		}
	
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(data.size());
			for (Entry<Integer, Double> e : data.entrySet()) {
				out.writeInt(e.getKey());
				out.writeDouble(e.getValue());
				System.out.printf("Write:size=%d,key=%d,v=%f\r\n",data.size(),e.getKey(),e.getValue());
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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CrystalBallHybird");

		job.setJarByClass(CrystalBallHybird.class);

		FileInputFormat.addInputPath(job, new Path("Input"));
		FileOutputFormat.setOutputPath(job, new Path("Output3"));
		

		job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		job.setPartitionerClass(MyPartitioner.class);
		// map output types
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		// reducer output types
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(StripeWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
