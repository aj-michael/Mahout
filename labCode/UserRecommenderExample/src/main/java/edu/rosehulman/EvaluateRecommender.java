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
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;

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
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1,similarity,model);
		RecommenderBuilder builder =  new RecommenderBuilder() {
			public Recommender buildRecommender(DataModel model) throws TasteException {
				UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
				UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1,similarity,model);
				return new GenericUserBasedRecommender(model,neighborhood,similarity);
			}
		};
	//	RecommenderBuilder builder = new SimpleRecommenderBuilder();
		return evaluator.evaluate(builder, null, model, 0.9, 1.0);
	}

}
