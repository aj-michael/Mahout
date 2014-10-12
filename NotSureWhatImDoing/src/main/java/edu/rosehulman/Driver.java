package edu.rosehulman;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.vectors.io.SequenceFileVectorWriter;
import org.apache.mahout.utils.vectors.io.VectorWriter;

import com.google.common.io.Files;


public class Driver {
	
	public static void main(String args[]) throws IOException {
		//Iterable<Vector> vectors = getVectors(args[0]);
		//writeVectors(vectors,args[1]);
		
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		Path inputPath = new Path(args[0]);
//		InputStreamReader in = new InputStreamReader(fs.open(inputPath));
//		BufferedReader reader = new BufferedReader(in);
		
//		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
//			System.out.println(line);
//		}
		for (File file : new File(args[0]).listFiles()) {
			for (String line : Files.toString(file, Charsets.UTF_8).split("\n")) {
				System.out.println(line);
				
			}
		}
		
	}

	@SuppressWarnings("deprecation")
	private static void writeVectors(Iterable<Vector> vectors, String path) throws IOException {
		URI outputURI = URI.create(path);
		Path outputPath = new Path(outputURI);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(outputURI,conf);
		SequenceFile.Writer sfwriter = SequenceFile.createWriter(fs,conf,outputPath, LongWritable.class, VectorWritable.class);
		VectorWriter vwriter = new SequenceFileVectorWriter(sfwriter);
		vwriter.close();
		vwriter.write(vectors);
	}
	
	private static Iterable<Vector> getVectors(String path) throws IOException {
		
		
		
		
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
