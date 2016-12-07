import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyzeStock {
	
	private static final String AGGREGATOR_KEY = "aggregator";
	private static final String FIELD_KEY = "field";
	private static final String START_DATE_KEY = "start_date";
	private static final String END_DATE_KEY = "end_date";
	
	private static HashMap<String, Integer> fieldMap = new HashMap<String, Integer>();
	
	static {
		fieldMap.put("high", 3);
		fieldMap.put("low", 4);
		fieldMap.put("close", 5);
	}
	
	public static boolean isDateWithin(Date date, Date startDate, Date endDate) {
		long d = date.getTime(), sd = startDate.getTime(), ed = endDate.getTime();
		return d > sd && d < ed;
	}
	
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, DoubleWritable> {

		private DoubleWritable wordValue = new DoubleWritable();
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			String field = conf.get(FIELD_KEY);
			String startDate = conf.get(START_DATE_KEY);
			String endDate = conf.get(END_DATE_KEY);
			String input[] = value.toString().split(",");
			String date = input[1];
			
			SimpleDateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
			SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
			Date sDate = null, eDate = null, cDate = null;
			try {
				if(startDate.contains("-"))
					sDate = df2.parse(startDate);
				else
					sDate = df1.parse(startDate);
				if(endDate.contains("-"))
					eDate = df2.parse(endDate);
				else
					eDate = df1.parse(endDate);
				if(date.contains("-"))
					cDate = df2.parse(date);
				else
					cDate = df1.parse(date);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			if(!isDateWithin(cDate, sDate, eDate))
				return;
			
			String fieldValue = input[fieldMap.get(field)];
			if(null == fieldValue || fieldValue.equals(""))
				return;
			double data = Double.valueOf(fieldValue);
			wordValue.set(data);
			word.set(input[0]);
			context.write(word, wordValue);
		}
	}

	public static class AggregatorReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String aggregator = conf.get(AGGREGATOR_KEY);
			int count = 0;
			double sum = 0;
			double minValue = Double.MAX_VALUE;
			double maxValue = Double.MIN_VALUE;
			for(DoubleWritable d: values) {
				double value = d.get();
				if(aggregator.equals("min")) {
					minValue = Math.min(value, minValue);
				} else if(aggregator.equals("max")) {
					maxValue = Math.max(value, maxValue);
				} else if(aggregator.equals("avg")) {
					sum += value;
					++count;
				}
			}
			if(aggregator.equals("min")) {
				result.set(minValue);
			} else if(aggregator.equals("max")) {
				result.set(maxValue);
			} else if(aggregator.equals("avg")) {
				result.set(sum/count);
			}
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(START_DATE_KEY, args[0]);
		conf.set(END_DATE_KEY, args[1]);		
		conf.set(AGGREGATOR_KEY, args[2]);
		conf.set(FIELD_KEY, args[3]);
		Job job = Job.getInstance(conf, "analyze stock");
		job.setJarByClass(AnalyzeStock.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(AggregatorReducer.class);
		job.setReducerClass(AggregatorReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		//job.setNumReduceTasks(Integer.valueOf(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[4]));
		FileOutputFormat.setOutputPath(job, new Path(args[5]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
