package infixExtractionNewIds;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class SuffixExtractionDriverNew {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(infixExtractionNewIds.SuffixExtractionDriverNew.class);

		conf.setJobName("SuffixExtractionNew");

//		conf.setMapOutputKeyClass(Text.class);
//		conf.setMapOutputValueClass(Text.class);
		
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(VIntWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));		
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(infixExtractionNewIds.SuffixExtractionMapper.class);		
	//	conf.setReducerClass(infixExtractionNewIds.SuffixExtractionReducer.class);
		
//		conf.setNumReduceTasks(224); //0.95 or 1.75 * (max_reducers overall = 6x15 = 90)
		conf.setNumReduceTasks(0); //0.95 or 1.75 * (max_reducers overall = 6x15 = 90)
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
