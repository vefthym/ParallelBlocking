package tokenBlocking;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

public class TokenBlockingEval {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(tokenBlocking.TokenBlockingEval.class);

		conf.setJobName("TokenBlockingD2 Evaluation");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
//		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class); //no output
		//conf.setOutputFormat(SequenceFileOutputFormat.class);
		//SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));		
//		FileOutputFormat.setOutputPath(conf, new Path(args[1])); 

		conf.setMapperClass(tokenBlocking.TokenBlockingMapperEvaluation.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(tokenBlocking.VIntArrayWritable.class);
		conf.setCompressMapOutput(true);
		conf.setReducerClass(tokenBlocking.TokenBlockingPairsReducer.class);
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		conf.setNumReduceTasks(300);
		//conf.setNumReduceTasks(0);
		
		try {
			DistributedCache.addCacheFile(new URI("/user/hduser/stopwordsD2.txt"), conf);
			//DistributedCache.addCacheFile(new URI("/user/hduser/unary.txt"), conf);
		} catch (URISyntaxException e1) {
			System.err.println(e1.toString());
		}

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
