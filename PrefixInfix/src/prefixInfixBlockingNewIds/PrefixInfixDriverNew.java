package prefixInfixBlockingNewIds;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class PrefixInfixDriverNew {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(prefixInfixBlockingNewIds.PrefixInfixDriverNew.class);

		conf.setJobName("PrefixInfixBlockingD3");
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(VIntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		//conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(conf, new Path(args[0]), SequenceFileInputFormat.class, prefixInfixBlockingNewIds.TokenMapper.class); //Entities
		MultipleInputs.addInputPath(conf, new Path(args[1]), SequenceFileInputFormat.class, IdentityMapper.class); //Infixes
		
		//FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		
		conf.setCombinerClass(prefixInfixBlockingNewIds.Combiner.class);
		conf.setReducerClass(prefixInfixBlockingNewIds.NonUniqueReducer.class); 
		conf.setNumReduceTasks(360); 

		try {
			DistributedCache.addCacheFile(new URI("/user/hduser/stopwordsD3.txt"), conf);			
		} catch (URISyntaxException e) {
			System.err.println(e.toString());
		}	

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
