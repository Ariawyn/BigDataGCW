import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WikiSearch {
    public static void runJob(String[] input, String output) throws Exception {
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance();
        job.setJarByClass(WikiSearch.class);
        
        job.setReducerClass(WikiSearchReducer.class);
        
        job.setMapperClass(WikiSearchMapper.class);
   
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //job.addCacheFile(new Path("./testinput/testmedalists.csv").toUri()); // For testing on local
        //job.addCacheFile(new Path("/data/medalistsrio.csv").toUri());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        Path outputPath = new Path(output);
        
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        
        job.waitForCompletion(true); 
    }

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}
