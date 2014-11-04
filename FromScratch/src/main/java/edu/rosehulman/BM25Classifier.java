package edu.rosehulman;

import com.datasalt.pangool.io.ITuple;

public class BM25Classifier extends AbstractClassifier {

	private final double b;
	private final double k;

	public BM25Classifier() {
		this(0.75,1.5);
	}
	
	public BM25Classifier(double b, double k){
		this.k = k;
		this.b = k;
	}

	protected double idf(long frequency, long vocabulary_size){
		return Math.log((vocabulary_size-frequency+0.5)/(frequency+0.5));
	}

	@Override
	protected double score(Iterable<ITuple> tuples, long vocabulary_size) {
		// TODO Auto-generated method stub
		double score = 0;
		for(ITuple tuple : tuples){			
			int wordcount = (Integer) tuple.get("count");
			double idfval = idf(wordcount,vocabulary_size);
			score += Math.max(
					0, 
					idfval*wordcount*(k+1)/(wordcount+k*(1-b+b*)
		}
		return 0;
	}

}
