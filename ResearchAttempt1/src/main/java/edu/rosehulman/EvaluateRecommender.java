package edu.rosehulman;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;

public class EvaluateRecommender {

	public static void main(String[] args) throws IOException, TasteException {
		int NUM_TRIALS = 10;
		List<Double> results = new ArrayList<Double>();
		for(;NUM_TRIALS > 0; NUM_TRIALS--) {
			results.add(trial());
		}
		System.out.println(results);
	}
	
	public static double trial() throws IOException, TasteException {
		DataModel model = new FileDataModel(new File("input/dataset.csv"));
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		RecommenderBuilder builder = new SimpleRecommenderBuilder();
		return evaluator.evaluate(builder, null, model, 0.9, 1.0);
	}

}
