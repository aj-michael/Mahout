package edu.rosehulman.dan;

import java.util.Set;

import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.ITuple;

public class NaiveBayesClassifier extends AbstractClassifier {

	private static final long serialVersionUID = 1L;

	protected double score(Iterable<ITuple> tuples, long vocabulary_size) {
		double score = 0;
		int words = 0;
		int yeartotalwords = 0;
		for (ITuple tuple : tuples) {
			if (tuple.getSchema().getName().equals("input schema")) {
				int wordcount = (Integer) tuple.get("count");
				score += Math.log(wordcount);
				words += 1;
			} else if (tuple.getSchema().getName().equals("count schema")) {
				yeartotalwords = (Integer) tuple.get("count");
			}
		}
	
		// this is an inner join
		if (words == 0) return Double.NaN;
		// per the formula
		score -= words * Math.log(yeartotalwords + vocabulary_size);
		return score;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new NaiveBayesClassifier(), args);
	}


	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}