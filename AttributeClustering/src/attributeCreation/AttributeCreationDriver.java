package attributeCreation;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class AttributeCreationDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(
				attributeCreation.AttributeCreationDriver.class);
		
		conf.setJobName("AttributeCreation D2");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //entities
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //attributes-output

		conf.setMapperClass(attributeCreation.AttributeMapperFromEntities.class);
		conf.setReducerClass(attributeCreation.AttributeReducerTrigrams.class);		
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		conf.setNumReduceTasks(180);
		
		try {
			DistributedCache.addCacheFile(new URI("/user/hduser/stopwordsD2.txt"), conf);  
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
