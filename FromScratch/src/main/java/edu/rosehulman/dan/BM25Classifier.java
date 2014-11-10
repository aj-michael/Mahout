package edu.rosehulman.dan;

import java.util.Set;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;


public class BM25Classifier extends AbstractClassifier {

	private static final long serialVersionUID = 1L;
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

	protected double score(Iterable<ITuple> tuples, long vocabulary_size){
		return 0;
	}

	protected void preprocess(TupleMRBuilder job) {
		// TODO Auto-generated method stub
		
	}

	protected double score(Iterable<ITuple> tuples, Set<String> words,
			long vocabulary_size) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
