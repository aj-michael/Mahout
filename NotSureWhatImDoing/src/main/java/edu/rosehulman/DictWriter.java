package edu.rosehulman;

import org.apache.hadoop.conf.Configuration;
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

    public static void main(String args[]) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(args[1]), Text.class, IntWritable.class);
        for (File file : new File(args[0]).listFiles()) {
            InputStream fileStream = new FileInputStream(file);
            InputStream gzipStream = new GZIPInputStream(fileStream);
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
                writer.append(new IntWritable(rank), new Text(word));
            }
        }
        buf.close();
        writer.close();
    }

}
