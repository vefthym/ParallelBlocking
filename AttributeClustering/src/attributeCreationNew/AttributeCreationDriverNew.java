package attributeCreationNew;

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

public class AttributeCreationDriverNew {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(
				attributeCreationNew.AttributeCreationDriverNew.class);
		
		conf.setJobName("AttributeCreation D2");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //entities
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //attributes-output

		conf.setMapperClass(attributeCreationNew.AttributeMapperFromEntities.class);
		conf.setReducerClass(attributeCreationNew.AttributeReducerTrigrams.class);		
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		conf.setNumReduceTasks(112);
		
//		try {
//			DistributedCache.addCacheFile(new URI("/user/hduser/stopwordsD2.txt"), conf);  
//		} catch (URISyntaxException e1) {
//			System.err.println(e1.toString());
//		}
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
