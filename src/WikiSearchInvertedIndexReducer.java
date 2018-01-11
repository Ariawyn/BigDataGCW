
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.util.HashMap;

import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;

public class WikiSearchInvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text wordKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // A hashmap that stores the article name as key
        // and the amount of occurances of the word as the value
        HashMap map = new HashMap();

        int count = 0;

        for(Text articleTitleText: values)
        {
            String title = articleTitleText.toString();

            if(map != null && map.get(title) != null)
            {
                count = (int)map.get(title);

                map.put(title, ++count);
            }
            else
            {
                map.put(title, 1);
            }
        }

        context.write(wordKey, new Text(map.toString()));
    
    }
}
