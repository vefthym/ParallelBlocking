package evaluation;

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

public class ACBEvalDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(evaluation.ACBEvalDriver.class);

		conf.setJobName("ACB Evaluation D3");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class); //no output
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		//FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		conf.setMapperClass(evaluation.ACBEvalMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(evaluation.VIntArrayWritable.class);
		conf.setCompressMapOutput(true);
		conf.setCombinerClass(evaluation.ACBEvalCombiner.class);		
		conf.setReducerClass(evaluation.ACBPairsReducer.class); 
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		conf.setNumReduceTasks(450);
		
		
		
		try {
			DistributedCache.addCacheFile(new URI("/user/hduser/stopwordsD3.txt"), conf);
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
