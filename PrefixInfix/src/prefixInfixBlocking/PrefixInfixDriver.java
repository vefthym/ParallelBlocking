package prefixInfixBlocking;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class PrefixInfixDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(prefixInfixBlocking.PrefixInfixDriver.class);

		conf.setJobName("PrefixInfixBlocking");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		//conf.setInputFormat(TextInputFormat.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));		
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		conf.setMapperClass(prefixInfixBlocking.PrefixInfixMapper.class);
		
		//no reducer
		conf.setReducerClass(IdentityReducer.class);
		conf.setNumReduceTasks(180); //just to sort the output
		//conf.setReducerClass(prefixInfixBlocking.PrefixInfixReducer.class); //only for testing

		//conf.setNumReduceTasks(31);  //only for testing
		
		
		try {
			DistributedCache.addCacheFile(new URI("/user/hduser/stopwordsNew.txt"), conf);
			DistributedCache.addCacheFile(new URI("/user/hduser/Infix-output.txt"), conf);
/*
			Path directoryPath = new Path(args[1]);
			//add the infixes in the Distributed Cache			
			FileSystem fs = FileSystem.get(new Configuration());			
				
			FileStatus[] fileStatus = fs.listStatus(directoryPath);
			for (FileStatus status : fileStatus) {				
				DistributedCache.addCacheFile(status.getPath().toUri(), conf);
				break; //FIXME: remove (added just for debugging)!!!!!!!!!!!!!!!!!!
			}			
		} catch (IOException e) {			
			System.err.println(e.toString());*/
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
