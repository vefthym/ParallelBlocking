package attributeSimilarities;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class BestMatchMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
	static enum InputData { NOT_A_VALID_KEY, NOT_A_VALID_SCORE };
	/**
	 * emit the input key-value pairs and the inverse (d1,d2-score) and (d2,d1-score) 
	 */
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		/*
		String [] keyParams = value.toString().split("\t");
		if (keyParams.length != 2) { //set counter for malformed input
			reporter.incrCounter(InputData.NOT_A_VALID_KEY, 1);
			System.out.println("Not a valid key: "+value);
			return;
		}
		String actualKey = keyParams[0];
		String actualValue = keyParams[1].trim();
		*/
		String actualKey = key.toString();
		String actualValue = value.toString().trim();
		String[] valuePair = actualValue.split(";;;");
		if (valuePair.length != 3) {
			reporter.incrCounter(InputData.NOT_A_VALID_SCORE, 1);
			System.out.println("Not a valid score: "+actualValue);
			return;
		}			
		reporter.progress();
		String pred2 = valuePair[0]+";;;"+valuePair[1];
		reporter.progress();
		String score = valuePair[2];
		output.collect(new Text(actualKey), new Text(actualValue));
		//add the inverse
		output.collect(new Text(pred2), new Text(actualKey+";;;"+score));
	}

}
