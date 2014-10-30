package edu.rosehulman;

import java.io.File;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class HundredRecommender {

	public static void main(String[] args) {
		DataModel model = null;
		UserSimilarity similarity = null;
		try {
			model = new FileDataModel(new File("input/ua.base"));
			similarity = new PearsonCorrelationSimilarity(model);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		UserNeighborhood neighborhood = null;
		try {
			neighborhood = new NearestNUserNeighborhood(75, similarity, model);
		} catch (TasteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
		
		// Recommend three items each for users 100 and 200
		List<RecommendedItem> user100Recommendations = null;
		List<RecommendedItem> user200Recommendations = null;
		
		try {
			user100Recommendations = recommender.recommend(100, 3);
			user200Recommendations = recommender.recommend(200, 3);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// Print out recommendations
		System.out.println("User 100 Recommendations:");
		for (int i = 0; i < 3; i++) {
			System.out.println("Movie: " + user100Recommendations.get(i));
		}
		
		System.out.println("\n-------------------\nUser 200 Recommendations:");
		for (int i = 0; i < 3; i++) {
			System.out.println("Movie: " + user200Recommendations.get(i));
		}
	}
	
}
