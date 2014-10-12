set output.compression.enabled true;
set output.compression.codec org.apache.hadoop.io.compress.GzipCodec
records = load '$input' as (word:chararray,year:int,c1:int,c2:int);
ranked = rank records by word;
sorted = order ranked by year;
store sorted into '$output' using PigStorage('\t');
