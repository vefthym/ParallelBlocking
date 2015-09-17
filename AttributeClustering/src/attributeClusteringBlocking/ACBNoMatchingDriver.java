package attributeClusteringBlocking;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class ACBNoMatchingDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(
				attributeClusteringBlocking.ACBNoMatchingDriver.class);

		conf.setJobName("AttributeClusteringBlockingD3");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		/*
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		*/
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		conf.setMapperClass(attributeClusteringBlocking.ACBMapperNoMatching.class);
		conf.setCombinerClass(attributeClusteringBlocking.ACBCombinerNoMatching.class);//TODO:check why Combiner causes OutOfMemory errors
		//conf.setReducerClass(attributeClusteringBlocking.ACBReducerNoMatching.class);
		conf.setNumReduceTasks(180);
		//conf.setReducerClass(IdentityReducer.class); //just to sort the output
		conf.setReducerClass(attributeClusteringBlocking.NonUniqueReducer.class); //just to sort the output
		
		try {
			DistributedCache.addCacheFile(new URI("/user/hduser/stopwordsD3.txt"), conf); 
			//DistributedCache.addCacheFile(new URI(args[1]+"/part-00000"), conf);			
			DistributedCache.addCacheFile(new URI(args[1]+"/"+"merges-r-00000"), conf);
			DistributedCache.addCacheFile(new URI(args[1]+"/"+"part-00000"), conf);
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
