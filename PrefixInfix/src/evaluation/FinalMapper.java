package evaluation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FinalMapper extends MapReduceBase implements Mapper<Text, VIntArrayWritable, Text, VIntArrayWritable> {

	public void map(Text key, VIntArrayWritable value,
			OutputCollector<Text, VIntArrayWritable> output, Reporter reporter) throws IOException {
		
		VIntWritable[] allValues = value.get();
		if (key.toString().startsWith("#")) {//don't know why this appears
			key = new Text (key.toString().substring(1));
		}		
		reporter.progress();
		String[] subject = key.toString().split(";;;");	
		
		int counter = 1;
		for (VIntWritable val: allValues) {			
			VIntWritable[] prevKeys = new VIntWritable[++counter]; //stores the previous keys
			prevKeys[0] = new VIntWritable(Integer.parseInt(subject[0])); //first is datasource iD 
			prevKeys[1] = new VIntWritable(Integer.parseInt(subject[1])); //second in the array is subject (entity id)
			reporter.progress();
			System.arraycopy(allValues, 0, prevKeys, 2, counter-2);		
			output.collect(new Text(Integer.toString(val.get())), new VIntArrayWritable(prevKeys));		
		}
		
	}

}
