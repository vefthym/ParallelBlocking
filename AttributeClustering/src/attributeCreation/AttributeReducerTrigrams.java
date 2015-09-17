package attributeCreation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import util.Tools;

public class AttributeReducerTrigrams extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		

		
		//from wikipedia - the last ones (after "men") are mine (web-specific)
		//String[] stop3gramsArray = {"the","and","tha","ent","ing","ion","tio","for","nde","has","nce","edt","tis","oft","sth","men", "www", "com", "org", "rdf", "xml", "owl"}; //FIXME: update dynamically
		//Set<String> stop3grams = new HashSet<>(Arrays.asList(stop3gramsArray));		
		
		Set<String> trigramsSet = new HashSet<>();
		while (values.hasNext()) { //this is the object of one triple (many words)			
			String value = values.next().toString(); //split the value field into tokens
			//value = value.replaceAll("\"@en", ""); //FIXME: update to remove stopwords
			//value = value.replaceAll("http://", "");		
			//value = value.replaceAll("xmlschema", "");
			//value = value.replaceAll("[^a-z0-9 ]+"," "); //remove special characters, keep white space//already in lowercase
			value = value.trim().replaceAll("[ ]+", "_"); //replace white spaces with '_'			
			//System.out.println(value);
			reporter.setStatus(_key+" starting trigramization...");
			Set<String> value3grams = Tools.getTrigrams(value);
			reporter.setStatus("Finished trigramization!");
			for (String trigram: value3grams) {
				//if (stop3grams.contains(trigram)) {	continue; }			
				trigramsSet.add(trigram);			
				reporter.progress();
			}			
		}
		reporter.setStatus("Start emitting...");
		
		if (trigramsSet.isEmpty()) {
			return;
		}
		
		String toEmit = "";
		for (String value : trigramsSet) {
			toEmit += " "+value;
			reporter.progress();
		}
		toEmit.trim();
		output.collect(_key, new Text(toEmit));
	}
}
