package com.grouph.wikisearch;

import java.util.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.grouph.wikisearch.InvertedIndexComparator;

public class Main {
    public static void main(String[] args) throws Exception {

        // Sanitise search term.
        String searchQuery = args[0].toLowerCase().trim();

        Configuration conf = new Configuration();
        // Set input file path to the output folder.
        Path inputPath = new Path("wikisearch-output/part-r-00000");

        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
        Reader.file(inputPath));

        Text key = new Text();
        Text value = new Text();

        // Run loop until no key value pairs left in sequence file.
        while (reader.next(key, value)) {

            // Skip to next key value pair if key does not match search query.
            if (!key.toString().equals(searchQuery)) continue;

            // Build title to frequency map for our word.
            HashMap<String, Integer> titleToFrequencyMap = new HashMap<>();

            // Turn value to a string and remove curly brackets.
            String titleToFrequencyMapString = value.toString();
            titleToFrequencyMapString = titleToFrequencyMapString.substring(1, titleToFrequencyMapString.length() - 1);

            // Split value in to title frequency pairs. Then split those in to title and frequency inside each loop.
            // Add to the new map with the correctly formatted title and frequency pairs.
            String[] titleToFrequencyPairString = titleToFrequencyMapString.split(", ");

            for (int i = 0; i < titleToFrequencyPairString.length; i++) {

                String[] keyAndValue = titleToFrequencyPairString[i].split("=");

                // If search term is within title, raise frequency enough to give a top ten ranking.
                // If search term is exactly the same as title, raise frequency to maximum to ensure first place in ranking.
                int frequency = Integer.parseInt(keyAndValue[1]);
                if(keyAndValue[0].toLowerCase().indexOf(searchQuery) != -1) frequency += 1000;
                if(keyAndValue[0].toLowerCase().equals(searchQuery)) frequency = Integer.MAX_VALUE;
                titleToFrequencyMap.put(keyAndValue[0], frequency);


            }

            // Put all pairs in our map into a priority queue.
            PriorityQueue<Map.Entry<String, Integer>> titlePriorityQueue = new PriorityQueue<>(titleToFrequencyMap.size(), new InvertedIndexComparator());
            for (Map.Entry<String, Integer> titleAndFrequency : titleToFrequencyMap.entrySet()) {
                titlePriorityQueue.add(titleAndFrequency);
            }

            // Get top 10 and output them. Break out of loop if there are not enough results.
            for (int i = 0; i < 10; i++) {
                if (titlePriorityQueue.peek() == null) break;
                System.out.println(titlePriorityQueue.poll().getKey());
            }

            // Close sequence file reader. Exit while loop.
            reader.close();
            break;
        }

    }
}
