package edu.rosehulman;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.Pair;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

public class Converter {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Path otherOutput = new Path(args[2]);
		Pair<Long[],List<Path>> pear = TFIDFConverter.calculateDF(input, output, conf, 500);
		TFIDFConverter.processTfIdf(input, otherOutput, conf, pear, 1, 99, PartialVectorMerger.NO_NORMALIZING, true, true, true, 1);
	}

}
