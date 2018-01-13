#!/bin/sh

cd $(dirname $0)/..

if [[ $# != 1 ]]; then
    echo "You need to call this script with a search query. Example:"
    echo "    ./run-search.sh hello"
    exit 1
fi

hadoop jar dist/WikiSearch.jar com.grouph.wikisearch.Main "$1"
