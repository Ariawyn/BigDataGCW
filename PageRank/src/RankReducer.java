import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;

public class RankReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException,InterruptedException{
     //stores initial pageRank
     String initialPageRank = "1.0";
     //sets boolean flag variable first to true
     boolean first = true;
     //for loop to iterate through each value in set of Text values
     for(Text value : values) {
      //checks if not first link article 
      if(!first) {
       initialPageRank += ",";
       //concatenates link article name to the end of current string 1.0 ,
       initialPageRank += value.toString();
       //sets first boolean flag to false
       first = false;
      }
      //emits article title as key and link article name as value 
      context.write(page,new Text(initialPageRank));
     }
    }
 }