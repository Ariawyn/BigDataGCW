package com.grouph.wikisearch;

import java.util.Comparator;
import java.util.Map;


// Sort in descending order based on frequency.

public class InvertedIndexComparator implements Comparator<Map.Entry<String, Integer>> {
    public int compare(Map.Entry<String, Integer> x, Map.Entry<String, Integer> y) {
        return y.getValue() - x.getValue();
    }
}
