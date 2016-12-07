import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyzeStockAdvanced {

	private static int getYear(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.YEAR);
	}

	public static class GroupByYearMapper extends
			Mapper<Object, Text, Text, Text> {

		private IntWritable year = new IntWritable();
		private DoubleWritable high = new DoubleWritable(),
				low = new DoubleWritable();
		private Text ticker = new Text(), mapperKey = new Text(),
				result = new Text();

		public void map(Object key, Text value, Context context) {
			try {
				String input[] = value.toString().split(",");
				String dateString = input[1];

				SimpleDateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
				SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
				Date date = null;
				try {
					if (dateString.contains("-"))
						date = df2.parse(dateString);
					else
						date = df1.parse(dateString);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				int y = getYear(date);
				year.set(y);
				ticker.set(input[0]);
				high.set(Double.valueOf(input[3]));
				low.set(Double.valueOf(input[4]));
				mapperKey.set(String.valueOf(y));
				result.set(input[0] + "," + high.get() + "," + low.get());
				context.write(mapperKey, result);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}

	public static class GroupByYearReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text r = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double high, low;
			String ticker = "", v = "";

			HashMap<String, Double[]> map = new HashMap<String, Double[]>();
			try {
				for (Text stock : values) {
					v = stock.toString();
					String[] a = v.split(",");
					high = Double.valueOf(a[1]);
					low = Double.valueOf(a[2]);
					ticker = a[0];
					Double[] d = new Double[2];
					if (map.containsKey(ticker)) {
						d = map.get(ticker);
						d[0] = Math.max(d[0], high);
						d[1] = Math.min(d[1], low);
					} else {
						d[0] = high;
						d[1] = low;
						map.put(ticker, d);
					}
				}

				String row;
				for (Entry<String, Double[]> entry : map.entrySet()) {
					Double[] d = entry.getValue();
					row = entry.getKey() + "," + d[0] + "," + d[1];
					r.set(row);
					context.write(key, r);
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public static class OutputMapper extends Mapper<Object, Text, Text, Text> {

		private Text year = new Text(), result = new Text();

		public void map(Object key, Text value, Context context) {
			try {
				String input[] = value.toString().split("\t");
				year.set(input[0]);
				result.set(input[1]);
				context.write(year, result);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}

	public static class OutputReducer extends Reducer<Text, Text, Text, Text> {
		private Text r = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double high = 0.0, low = 0.0, diff1, diff2;
			String ticker = "", v = "";

			try {
				for (Text stock : values) {
					v = stock.toString();
					String[] a = v.split(",");
					diff1 = Double.valueOf(a[1]) - Double.valueOf(a[2]);
					diff2 = high - low;
					if(diff1 > diff2) {
						high = Double.valueOf(a[1]);
						low = Double.valueOf(a[2]);
						ticker = a[0];
					}
				}
				String row = ticker + "\t" + low + "\t" + high + "\t" + (high - low);
				r.set(row);
				context.write(key, r);
			} catch (Exception ex) {
				r.set(v);
				context.write(key, r);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		String temp1[] = args[1].split("/");
		String tempInputPath = "";
		for (int i = 0; i < temp1.length - 1; ++i)
			tempInputPath += temp1[i] + "/";
		tempInputPath += "tempanalyzestockadvanced1";

		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		if (fs.exists(new Path(tempInputPath))) {
			fs.delete(new Path(tempInputPath), true);
		}
		
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		 
		// Job 1
		Job job = Job.getInstance(conf, "StockAnalyzerAdvanced");
		job.setJarByClass(AnalyzeStockAdvanced.class);
		job.setMapperClass(GroupByYearMapper.class);
		job.setCombinerClass(GroupByYearReducer.class);
		job.setReducerClass(GroupByYearReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setNumReduceTasks(Integer.valueOf(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(tempInputPath));
		job.waitForCompletion(true);

		// Job 2
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "StockAnalyzerAdvanced1");
		job1.setJarByClass(AnalyzeStockAdvanced.class);
		job1.setMapperClass(OutputMapper.class);
		job1.setCombinerClass(OutputReducer.class);
		job1.setReducerClass(OutputReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		// job.setNumReduceTasks(Integer.valueOf(args[0]));
		FileInputFormat.addInputPath(job1, new Path(tempInputPath));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
