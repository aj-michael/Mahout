data = LOAD '$input' USING PigStorage('\t') AS (word:chararray, year:chararray, occurances:int, numBooks:int);

REGISTER StripPOS-1.0.jar;

stripped = FOREACH data GENERATE edu.rosehulman.StripPOS(word) AS word, year, occurances, numBooks;
grouplicates = GROUP stripped BY (word, year);
test = FOREACH grouplicates GENERATE group, SUM(stripped.occurances) as occurances, SUM(stripped.numBooks) as numBooks;
testFlatten = FOREACH test GENERATE FLATTEN(group), occurances, numBooks;
STORE testFlatten INTO '$output' USING PigStorage('\t');