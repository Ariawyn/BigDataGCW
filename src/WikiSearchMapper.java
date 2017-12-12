import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;


public class WikiSearchMapper extends Mapper<IntWritable, EnglishWikipediaPage, IntWritable, EnglishWikipediaPage> { 
    public void map(IntWritable nid, EnglishWikipediaPage page,	Context context) throws IOException, InterruptedException {
	context.write(nid, page);
    }
}
