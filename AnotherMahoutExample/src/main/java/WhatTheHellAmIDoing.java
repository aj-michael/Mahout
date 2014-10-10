import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.naivebayes.test.BayesTestMapper;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WhatTheHellAmIDoing extends AbstractJob {
	
	private static final Logger log = LoggerFactory.getLogger(WhatTheHellAmIDoing.class);
	
	public static final String LABEL_KEY = "labels";
	public static final String COMPLEMENTARY = "class";
	

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WhatTheHellAmIDoing(), args);
	}

	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();
		addOption(addOption(DefaultOptionCreator.overwriteOption().create()));
		addOption("model", "m", "The path to the model built during training",
				true);
		addOption(buildOption("testComplementary", "c", "test complementary?",
				false, false, String.valueOf(false)));
		Map<String, List<String>> parsedArgs = parseArguments(args);
		if (parsedArgs == null) {
			return -1;
		}
		if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
			HadoopUtil.delete(getConf(), getOutputPath());
		}
		Path model = new Path(parsedArgs.get("--model"));
		Path model = 
		HadoopUtil.cacheFiles(model, getConf());
		Job job = prepareJob(getInputPath(), getOutputPath(),
				SequenceFileInputFormat.class, BayesTestMapper.class,
				Text.class, VectorWritable.class,
				SequenceFileOutputFormat.class);
		boolean complementary = parsedArgs.containsKey("--testComplementary");
		job.getConfiguration().set(COMPLEMENTARY, String.valueOf(complementary));
		return 0;
	}

}
