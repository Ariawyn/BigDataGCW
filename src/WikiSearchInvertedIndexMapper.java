import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;


public class WikiSearchInvertedIndexMapper extends Mapper<IntWritable, EnglishWikipediaPage, Text, Text> { 
    public void map(IntWritable nid, EnglishWikipediaPage page,	Context context) throws IOException, InterruptedException {
	// Make sure the page we are checking is actually and article
        if(page != null)
        {

            if(page.isArticle())
            {
                try
                {
                    // Get the string content and title of the english wikipedia page, the content is title of the article + the actual content
                    String title = page.getTitle();
                    String content = page.getContent();

                    // We want to be case insensitive, so we will do everything in lowercase
                    content = content.toLowerCase();

                    // We want to get all the words in the content so we split the string
                    String[] words = content.split("\\W+");

                    // Now, for every word, we want to emit the word as key, and the article title    
                    for(String word : words) {
                        // First, check for any punctuation or some such that might make the word not be correct
                        word = word.replaceAll("[^\\w]", "");

                        // Then, now we have to check whether the word is valid
                        // We do this by checking if it has anything other than non alphanumeric characters
                        boolean isValid = !word.matches("^.*[^a-zA-Z0-9 ].*$");

                        // We also want to check if it isnt empty
                        if(word == null || word.isEmpty() || word.length() <= 3)
                        {
                            return;
                        }

                        if(isValid)
                        {
                            context.write(new Text(word), new Text(title));
                        }
                    }
                }
                catch(NullPointerException e)
{
                    System.out.println("NullPointerException somewhere, probably with the getContent() function of the englishwikipediapage class");
                }
            }
        }
    }
}
