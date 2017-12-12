import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WikiSearchReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    public IntWritable count = new IntWritable();
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {     

        int sum = 0;

        for(IntWritable partialCount: values){
            sum += partialCount.get();
        }
        
        count.set(sum);
        
        
        context.write(key, count);
		
    }
}
