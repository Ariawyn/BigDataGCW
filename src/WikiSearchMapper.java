import java.io.IOException;
import java.lang.Math;
import org.apache.hadoop.io.IntWritable;

import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiSearchMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
  
    private IntWritable one = new IntWritable(1);

    private ArrayList<String> athletes;
    private Hashtable<String, String> athleteToSport;

    protected void setup(Context context) throws IOException, InterruptedException {
        athletes = new ArrayList<String>();
        athleteToSport = new Hashtable<String, String>();

        URI fileUri = context.getCacheFiles()[0];

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(new Path(fileUri));

        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String currentLine = null;
        try {
            br.readLine();

            while((currentLine = br.readLine()) != null) {
                String[] fields = currentLine.split(",");

                // Fields are as follows for this data set:
                // id, name, nationality, sex, dob, height, weight, sport, gold, silver, bronze
                
                if(fields.length == 11) {   // We want only valid data
                    if(fields[1].length() > 0 && fields[7].length() > 0) { // Check if there exists an entry for the field and that it is not empty
                        // Add athlete name to list of athletes
                        athletes.add(fields[1]);

                        // Add entry to hashtable associating athlete to their sport
                        athleteToSport.put(fields[1], fields[7]);
                    }
                }
            }

            System.out.println("ArrayList for athletes is size: " + athletes.size());

        } catch (IOException e) {
            System.out.println("Failed to read dataset into cache");
        }
    }
    
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
      
	String[] fields = line.toString().split(";");  
        
        //Fields contains line as follows. 
        //0          1     2     3    
        //epoch_time;tweetId,tweet,device
        
        if(fields.length == 4) {

            String tweettext = fields[2];

            if(tweettext.length() <= 140) {         // Checking to make sure the tweet is valid in length
                
                for(String athlete: athletes) {
                    
                    if(tweettext.contains(athlete)) {
                        context.write(new Text( athleteToSport.get(athlete) ), one);
                    }

                }
            }
        }
    }
}
