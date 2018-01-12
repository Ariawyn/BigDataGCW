
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

        // For every article title assoiciated with the wordKey
        for(Text articleTitleText: values)
        {

            // We want to get the title in a string value
            String title = articleTitleText.toString();

            // Then check if the article title already exists
            if(map.get(title) != null)
            {
                // If it does we want to get the count of times the word has already appeared in that specified article
                // Note: this has to be casted to an int as the hashmap type stores its value as just an Object type
                int count = (int) map.get(title);

                // and increment it
                count = count + 1;

                // then add it back to the map
                map.put(title, count);
            }
            else
            {
                // If no record of the article exists in the map for this key word, then we create one and set it to 1
                map.put(title, 1);
            }
        }

        // Then we write to the context the word key, and its associated map of frequency in any articles it appeared in
        // this essentially creates the inverted index
        context.write(wordKey, new Text(map.toString()));
    }
}
