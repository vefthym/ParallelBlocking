package infixExtraction;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

public class ChainInfixSuffixDriver {

	public static void main(String[] args) {
		
		//Infix Extraction		
		JobConf conf = new JobConf(infixExtraction.InfixExtractionDriver.class);

		conf.setJobName("InfixExtraction");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
//		conf.setInputFormat(TextInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(infixExtraction.InfixExtractionMapper.class);
		conf.setReducerClass(infixExtraction.InfixExtractionReducer.class);
		
		conf.setNumReduceTasks(31); //0.95 or 1.75 * (max_reducers overall = 3x6 + 5x3 = 33)
		
		
		//Suffix Extraction
		JobConf conf2 = new JobConf(infixExtraction.SuffixExtractionDriver.class);
		
		conf2.setJobName("SuffixExtraction");

		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);
		
//		conf2.setInputFormat(TextInputFormat.class);
//		conf2.setOutputFormat(TextOutputFormat.class);
		
		conf2.setInputFormat(SequenceFileInputFormat.class);
		conf2.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf2, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
		
		conf2.setMapperClass(infixExtraction.SuffixExtractionMapper.class);
		conf2.setReducerClass(infixExtraction.SuffixExtractionReducer.class);
		
		conf2.setNumReduceTasks(31); //0.95 or 1.75 * (max_reducers overall = 3x6 + 5x3 = 33)
		
		
		//runAll
		try {
			JobControl control = new JobControl("InfixSuffixExtraction");			
			
			Job infixExtraction = new Job(conf);			
			control.addJob(infixExtraction);
			
			Job suffixExtraction = new Job(conf2);
			suffixExtraction.addDependingJob(infixExtraction);
			control.addJob(suffixExtraction);								
			
			control.run();			
		} catch (IOException e1) {
			System.err.println(e1.toString());
		}
	}

}
