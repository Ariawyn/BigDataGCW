import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage.Link;
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;

public class RankMapper extends Mapper <IntWritable, EnglishWikipediaPage,Text,Text> {
	
  public void map(IntWritable nid, EnglishWikipediaPage page,Context context)throws IOException, InterruptedException
  {
	//first checks if the WikipediaPage object is not = to null
	if(page != null) {
     // if statement to check if page is an article
     if(page.isArticle()) {
      //retrieves article title
      String pageName = page.getTitle();
      //extracts all page links from the wiki article page, stores them in a Linked List
      List<Link>pageLinks = page.extractLinks();
      //for statement to iterate through each link stored in the linked list
      for(int i=0; i<pageLinks.size(); i++) {
       //converts each page link to string
       String pageLink = pageLinks.get(i).toString();
       //emits the article title as key and link page title as value
       context.write(new Text(pageName),new Text(pageLink));	  
      }
     }
     return;
	}
  }
}
