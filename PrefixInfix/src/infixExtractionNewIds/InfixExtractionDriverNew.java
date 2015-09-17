package infixExtractionNewIds;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class InfixExtractionDriverNew {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(infixExtractionNewIds.InfixExtractionDriverNew.class);
		
		conf.setJobName("InfixExtractionNew");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		
		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, infixExtractionNewIds.InfixExtractionMapperFromSubject.class); //infix of subject //the mappings files
		MultipleInputs.addInputPath(conf, new Path(args[1]), SequenceFileInputFormat.class, infixExtractionNewIds.InfixExtractionMapper.class); //infixes in values //the entityIds files
		
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		
		conf.setReducerClass(infixExtractionNewIds.InfixExtractionReducer.class);			
		
		conf.setNumReduceTasks(280);
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
				
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
