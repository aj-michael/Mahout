package edu.rosehulman;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.net.URI;
import java.util.zip.GZIPInputStream;

/**
 * Created by adam on 10/19/14.
 */
public class DictWriter {

    @SuppressWarnings("deprecation")
	public static void main(String args[]) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(args[1]), Text.class, IntWritable.class);
        for (FileStatus fileStatus : fs.listStatus(new Path(args[0]))) {
        	Path p = fileStatus.getPath();
        	System.out.println("DictWriter - Processing file " + p.getName());
        	FSDataInputStream dis = fs.open(p);
            InputStream gzipStream = new GZIPInputStream(dis);
            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
            BufferedReader buf = new BufferedReader(decoder);
            String previousWord = "";
            for (String line = buf.readLine(); line != null; line = buf.readLine()) {
                String[] vals = line.split("\t");
                int rank = Integer.parseInt(vals[0]);
                String word = vals[1];
                if (word.equals(previousWord))
                    continue;
                previousWord = word;
                writer.append(new Text(word), new IntWritable(rank));
            }
            buf.close();
        }
        writer.close();
    }

}
