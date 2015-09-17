package util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class Uncompress {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(util.Uncompress.class);
			
		conf.setJobName("Uncompress");
		conf.setJarByClass(IdentityMapper.class);
		
		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		
		conf.setNumReduceTasks(100);
		
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		
		
		SequenceFileInputFormat.addInputPath(conf, new Path(args[0]));
		TextOutputFormat.setOutputPath(conf, new Path(args[1]));
			
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
