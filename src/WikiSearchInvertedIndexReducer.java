
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;

public class WikiSearchInvertedIndexReducer extends Reducer<IntWritable, EnglishWikipediaPage, IntWritable, EnglishWikipediaPage> {
    public void reduce(IntWritable nid, Iterable<EnglishWikipediaPage> values,	Context context) throws IOException, InterruptedException {
        context.write(nid, values.iterator().next());
    }
}
