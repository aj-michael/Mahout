package edu.rosehulman;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.vectors.io.SequenceFileVectorWriter;
import org.apache.mahout.utils.vectors.io.VectorWriter;


public class Driver {
	
	public static void main(String args[]) throws IOException {
		Iterable<Vector> vectors = getVectors(args[0]);
		writeVectors(vectors,args[1]);
	}

	@SuppressWarnings("deprecation")
	private static void writeVectors(Iterable<Vector> vectors, String path) throws IOException {
		URI outputURI = URI.create(path);
		Path outputPath = new Path(outputURI);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(outputURI,conf);
		SequenceFile.Writer sfwriter = SequenceFile.createWriter(fs,conf,outputPath, LongWritable.class, VectorWritable.class);
		VectorWriter vwriter = new SequenceFileVectorWriter(sfwriter);
		vwriter.write(vectors);
		vwriter.close();
	}
	
	private static Iterable<Vector> getVectors(String path) throws IOException {
		
		
		for (File file : new File(path).listFiles()) {
			
		}
		
		
		Path inputPath = new Path(path);
		FileSystem fs = FileSystem.get(new Configuration());
		InputStreamReader in = new InputStreamReader(fs.open(inputPath));
		BufferedReader br = new BufferedReader(in);
		String line = br.readLine();
		while (line != null) {
			System.out.println(line);
			line = br.readLine();
		}
		return new ArrayList<Vector>();
	}
	

}
