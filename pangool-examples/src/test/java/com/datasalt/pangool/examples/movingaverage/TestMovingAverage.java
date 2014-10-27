/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.examples.movingaverage;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

public class TestMovingAverage extends AbstractHadoopTestLibrary{
	
	private final static String INPUT = "test-input-" + TestMovingAverage.class.getName();
	private final static String OUTPUT = "test-output-" + TestMovingAverage.class.getName();
	
	@Test
	public void test() throws Exception {
		trash(OUTPUT);
		
		Files.write(
				"url1" + "\t" + "2011-02-28" + "\t" + "100" + "\n" +
				"url1" + "\t" + "2011-02-27" + "\t" + "50" + "\n" +
				"url1" + "\t" + "2011-02-24" + "\t" + "25" + "\n" +
				"url2" + "\t" + "2011-02-28" + "\t" + "100" + "\n" +
				"url2" + "\t" + "2011-02-26" + "\t" + "50" + "\n" +
				"url2" + "\t" + "2011-02-25" + "\t" + "25" + "\n"
		, new File(INPUT), Charset.forName("UTF-8"));

		ToolRunner.run(getConf(),new MovingAverage(), new String[] { INPUT, OUTPUT, "3" });

		int validatedOutputLines = 0;
		for(String line : Files.readLines(new File(OUTPUT + "/part-r-00000"), Charset.forName("UTF-8"))) {
			String[] fields = line.split("\t");
			if(fields[0].equals("url1")) {
				if(fields[1].equals("2011-02-28")) {
					assertEquals("75.0", fields[2]);
					validatedOutputLines++;
				} else if(fields[1].equals("2011-02-27")) {
					assertEquals("50.0", fields[2]);
					validatedOutputLines++;
				} else if(fields[1].equals("2011-02-24")) {
					assertEquals("25.0", fields[2]);
					validatedOutputLines++;
				}
			} else if(fields[0].equals("url2")) {
				if(fields[1].equals("2011-02-28")) {
					assertEquals("75.0", fields[2]);
					validatedOutputLines++;
				} else if(fields[1].equals("2011-02-26")) {
					assertEquals("37.5", fields[2]);
					validatedOutputLines++;
				} else if(fields[1].equals("2011-02-25")) {
					assertEquals("25.0", fields[2]);
					validatedOutputLines++;
				}				
			}
		}
		
		assertEquals(6, validatedOutputLines);

		trash(INPUT, OUTPUT);
	}
}
