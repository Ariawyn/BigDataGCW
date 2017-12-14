ant clean dist
rm consoleoutput.txt
rm -r testout
hadoop-local jar dist/WikiSearch.jar WikiSearch testinput testout > consoleoutput.txt
vim consoleoutput.txt
rm consoleoutput.txt
