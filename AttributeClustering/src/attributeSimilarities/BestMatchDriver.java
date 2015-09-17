package attributeSimilarities;

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


public class BestMatchDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(attributeSimilarities.BestMatchDriver.class);

		conf.setJobName("BestMatch");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(VIntWritable.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//conf.setOutputFormat(SequenceFileOutputFormat.class);
		//SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		
		MultipleOutputs.addNamedOutput(
				conf, "merges",TextOutputFormat.class, IntWritable.class, VIntWritable.class);
		//MultipleOutputs.addNamedOutput(
			//	conf, "tmpclusters",TextOutputFormat.class, Text.class, VIntWritable.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(attributeSimilarities.BestMatchMapper.class);
		conf.setCombinerClass(attributeSimilarities.BestMatchCombiner.class);
		conf.setReducerClass(attributeSimilarities.BestMatchReducer.class);
		
		conf.setNumReduceTasks(1); //one reducer only!

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
