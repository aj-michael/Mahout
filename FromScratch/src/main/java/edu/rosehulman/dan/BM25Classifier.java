package edu.rosehulman.dan;

import com.datasalt.pangool.io.ITuple;

public class BM25Classifier  {

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

}
