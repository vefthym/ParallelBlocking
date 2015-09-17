package evaluation;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class PrefixInfixEvalDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(evaluation.PrefixInfixEvalDriver.class);

		conf.setJobName("PrefixInfixBlocking D1 Evaluation");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(evaluation.VIntArrayWritable.class);
		
		//conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		
		MultipleInputs.addInputPath(conf, new Path(args[0]), SequenceFileInputFormat.class, evaluation.PrefixInfixEvalMapper.class); //Entities
		MultipleInputs.addInputPath(conf, new Path(args[1]), SequenceFileInputFormat.class, evaluation.InfixesEvalMapper.class); //Infixes
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(VIntWritable.class);

		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		conf.setReducerClass(evaluation.ValuesReducer.class); 
		conf.setNumReduceTasks(360); 

		try {
			DistributedCache.addCacheFile(new URI("/user/hduser/stopwordsD1.txt"), conf);			
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
