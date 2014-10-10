import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.bayes.TrainClassifier;
import org.apache.mahout.classifier.bayes.algorithm.BayesAlgorithm;
import org.apache.mahout.classifier.bayes.common.BayesParameters;
import org.apache.mahout.classifier.bayes.datastore.InMemoryBayesDatastore;
import org.apache.mahout.classifier.bayes.exceptions.InvalidDatastoreException;
import org.apache.mahout.classifier.bayes.interfaces.Algorithm;
import org.apache.mahout.classifier.bayes.interfaces.Datastore;
import org.apache.mahout.classifier.bayes.model.ClassifierContext;
import org.apache.mahout.common.nlp.NGrams;

public class Starter {

	public static void main(String[] args) throws IOException,
			InvalidDatastoreException {
		final BayesParameters params = new BayesParameters();
		params.setGramSize(1);
		params.set("verbose", "true");
		params.set("classifierType", "bayes");
		params.set("defaultCat", "OTHER");
		params.set("encoding", "UTF-8");
		params.set("alpha_i", "1.0");
		params.set("dataSource", "hdfs");
		params.set("basePath", "/tmp/output");

		Path input = new Path("/tmp/input");
		Path output = new Path("/tmp/output");
		TrainClassifier.trainNaiveBayes(input, output, params);

		Algorithm algorithm = new BayesAlgorithm();
		Datastore datastore = new InMemoryBayesDatastore(params);

		ClassifierContext classifier = new ClassifierContext(algorithm,
				datastore);
		classifier.initialize();

		final BufferedReader reader = new BufferedReader(
				new FileReader(args[0]));
		String entry = reader.readLine();

		while (entry != null) {
			List<String> document = new NGrams(entry, Integer.parseInt(params
					.get("gramsSize"))).generateNGramsWithoutLabel();
			ClassifierResult result = classifier.classifyDocument(
					document.toArray(new String[document.size()]),
					params.get("defaultCat"));
			entry = reader.readLine();
		}
	}

}
