package edu.rosehulman;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class HundredEvaluator {

	public static void main(String[] args) {
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		
		RecommenderBuilder builder =  new RecommenderBuilder() {
			public Recommender buildRecommender(DataModel model) throws TasteException {
//				try {
//					model = new FileDataModel(new File("input/ua.base"));
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
				UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
				UserNeighborhood neighborhood = new NearestNUserNeighborhood(100,similarity,model);
				return new GenericUserBasedRecommender(model,neighborhood,similarity);
			}
		};
		
		try {
			DataModel model = new FileDataModel(new File("input/ua.base"));
			
		//	double differenceSum = 0;
			for (int i = 0; i < 3; i++) 
				System.out.println(evaluator.evaluate(builder, null, model, 0.9, 1.0));
			
			//System.out.println("Avg Dif over 10 Trials: " + differenceSum/10);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
