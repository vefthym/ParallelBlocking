package attributeSimilaritiesNew;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;


public class BestMatchDriverNew {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(attributeSimilaritiesNew.BestMatchDriverNew.class);

		conf.setJobName("BestMatch");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(VIntWritable.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);		
		
		MultipleOutputs.addNamedOutput(
				conf, "merges",TextOutputFormat.class, IntWritable.class, VIntWritable.class);		

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(attributeSimilaritiesNew.BestMatchMapper.class);
		conf.setCombinerClass(attributeSimilaritiesNew.BestMatchCombiner.class);
		conf.setReducerClass(attributeSimilaritiesNew.BestMatchReducer.class);
		
		conf.setNumReduceTasks(1); //one reducer only!

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
