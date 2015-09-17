package evaluation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

public class FinalPISEvalDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(evaluation.PrefixInfixEvalDriver.class);

		conf.setJobName("PIS Evaluation 2nd round Dirty");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(evaluation.VIntArrayWritable.class);
		

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class); //no output

		FileInputFormat.setInputPaths(conf, new Path(args[0]));		

		conf.setMapperClass(evaluation.FinalMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(evaluation.VIntArrayWritable.class);
//		conf.setCompressMapOutput(true);
		conf.setReducerClass(evaluation.PISPairsReducer.class);
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		conf.setNumReduceTasks(450);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
