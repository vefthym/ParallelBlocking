package infixExtractionNew;

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

public class InfixExtractionDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(infixExtractionNew.InfixExtractionDriver.class);
		
		conf.setJobName("InfixExtractionNew");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
//		conf.setInputFormat(TextInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));		
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(infixExtractionNew.InfixExtractionMapper.class);
//		conf.setCombinerClass(infixExtractionNew.InfixExtractionCombiner.class);
		conf.setReducerClass(infixExtractionNew.InfixExtractionReducer.class);			
		
		conf.setNumReduceTasks(360); //0.95 or 1.75 * (max_reducers overall = 15x6 = 90)
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
				
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
