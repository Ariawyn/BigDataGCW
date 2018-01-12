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
            // We check the page is actually an article
            if(page.isArticle())
            {
                // This try catch block is used so that we do not have to deal with NullPointerExceptions that have been
                // getting thrown at us from the cloud9 library. Currentl unsure of the cause, but cant fix it given its from
                // the library function.
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
                        // We also do not want to check any words that arent of sufficient length, this is mostly
                        // because of server memory restrictions
                        if(word == null || word.isEmpty() || word.length() <= 3)
                        {
                            return;
                        }

                        // If our word is in fact validated, we then write to the content: the word, and then the title of the article
                        if(isValid)
                        {
                            context.write(new Text(word), new Text(title));
                        }
                    }
                }
                catch(NullPointerException e)
                {
                    // Here we catch any and all null pointer exceptions, all of them probably being thrown by the cloud9 wikipediapage getContent() library function
                    System.out.println("NullPointerException somewhere, probably with the getContent() function of the englishwikipediapage class");
                }
            }
        }
    }
}
