package tokenBlocking;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TokenBlockingDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(tokenBlocking.TokenBlockingDriver.class);

		conf.setJobName("TokenBlockingD3");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputValueClass(VLongWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //input path in HDFS (entities)
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //output path in HDFS (blocks)

		conf.setMapperClass(tokenBlocking.TokenBlockingMapper.class);
		conf.setReducerClass(tokenBlocking.TokenBlockingGroupValuesReducer.class);
		
		conf.setNumReduceTasks(360);	
		/*
		try {
			DistributedCache.addCacheFile(new URI("/user/hduser/stopwordsD3.txt"), conf); //path in HDFS	
		} catch (URISyntaxException e1) {
			System.err.println(e1.toString());
		}
		*/
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
