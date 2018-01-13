#!/bin/sh

cd $(dirname $0)/..

hadoop jar dist/WikiSearch.jar com.grouph.wikisearch.WikiSearchInvertedIndex wikisearch-input wikisearch-output
