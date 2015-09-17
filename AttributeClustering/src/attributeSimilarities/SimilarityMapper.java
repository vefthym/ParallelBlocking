package attributeSimilarities;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SimilarityMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		
	int totalMappers;
	private int currentMapper;
	static enum InputData { NOT_A_VALID_KEY };
	
	public void configure(JobConf conf) {		
		totalMappers = Integer.parseInt(conf.get("mapred.map.tasks")); //only works with the jar, not from eclipse-plugin!!!
        currentMapper = Integer.parseInt(conf.get("mapred.task.partition"));
     //   System.out.println("totalMappers: "+totalMappers);
//		System.out.println("currentMapper: "+currentMapper);
	}
	
	/**
	 * input = the output of AttributesMapper:
	 * 	input key: datasourceID;;;predicate
	 * 	input value: all the values of this predicate (taking data source into account)
	 * output
	 * 	key: a pair determined by totalMappers and currentMapper (separated by underslash"_")
	 * 	value: mapperID;;;a copy of the input (both input key and input value separated by ";;;")
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
						
		String toEmit = currentMapper+";;;"+actualKey+";;;"+actualValue;		
		//System.out.println("toEmit: "+toEmit);
		for (int i = 0; i < totalMappers; ++i) {			
			if (currentMapper < i) {
				//System.out.println("emitting key: "+currentMapper+"_"+i+" for pred:"+actualKey);
				output.collect(new Text(Integer.toString(currentMapper)+"_"+i), new Text(toEmit));
			} else {
				//System.out.println("emitting key: "+i+"_"+currentMapper+" for pred:"+actualKey);
				output.collect(new Text(i+"_"+Integer.toString(currentMapper)), new Text(toEmit));
			}
		}		
	}
}
