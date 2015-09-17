package attributeSimilarities;

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

public class SimilarityDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(
				attributeSimilarities.SimilarityDriver.class);
		
		conf.setJobName("AttributeSimilarity");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");

		/*
		try{
			FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] statuses = fs.listStatus(new Path(args[0]));
            for (FileStatus status : statuses){
            	String fileName = status.getPath().getName();
    			if (!fileName.startsWith("part") || fileName.endsWith("~") || fileName.endsWith(".crc")) {
    				System.out.println("Deleting file: "+fileName);
    				fs.delete(status.getPath(), true);
    			}
            }
		}catch(Exception e){
			System.err.println(e.toString());
		}
		*/
		/*
		//local		
		File inputFolder = new File(args[0]);	
		for (File file : inputFolder.listFiles()){ 
			//System.out.println(file.getName());
			if (file.getName().endsWith(".crc") || 
					file.getName().endsWith("~") || 
					file.getName().startsWith("_")) {
				file.delete();
			}
		}
		*/

		conf.setMapperClass(attributeSimilarities.SimilarityMapper.class);
		conf.setReducerClass(attributeSimilarities.SimilarityReducer.class);
		
		conf.setNumReduceTasks(360); //nodes(=15), mapred.tasktracker.tasks.maximum(=6))
				
		client.setConf(conf);		
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
