package attributeClustering;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ClusteringDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(attributeClustering.ClusteringDriver.class);

		conf.setJobName("FinalClustering");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);	

		//FileInputFormat.setInputPaths(conf, new Path("/home/hduser/Documents/BestMatch-output/tmpclusters*"));//local
		FileInputFormat.setInputPaths(conf, new Path(args[0]+"/"+"part*"));//HDFS
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		try {
			DistributedCache.addCacheFile(new URI(args[0]+"/"+"merges-r-00000"), conf); 
		} catch (URISyntaxException e1) {
			System.err.println(e1.toString());
		}

		conf.setMapperClass(attributeClustering.ClusteringMapper.class);
		//no reducer		

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
