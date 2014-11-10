package edu.rosehulman.dan;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;

public class TupleFilePrinter {

	public void main(String[] args) throws IOException{
		Path p = new Path(args[0]);
		Configuration conf = new Configuration();
		FileSystem f = FileSystem.get(conf);
		TupleFile.Reader reader = new TupleFile.Reader(f, conf, p);
		ITuple tup = new Tuple(reader.getSchema());
		while(reader.next(tup)){
			System.out.println(tup);
		}
		reader.close();
	}
}
