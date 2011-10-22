/**
 * Copyright (C) 2011 AIM III course DIMA TU Berlin
 *
 * This programm is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.dima.aim.exercises.one;

import de.tuberlin.dima.aim.exercises.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.util.Map;

public class AverageTemperaturePerMonth extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

    Job avgTempJob = prepareJob(inputPath, outputPath, TextInputFormat.class, AvgTempPerMonthMapper.class,
            Text.class, IntWritable.class, AvgTempPerMonthReducer.class, Text.class, FloatWritable.class,
            TextOutputFormat.class);
    
    avgTempJob.getConfiguration().set("minQuality", "" + minimumQuality);
    avgTempJob.waitForCompletion(true);
    
    return 0;
  }
  
  public static class AvgTempPerMonthMapper extends Mapper<Object, Text, Text, IntWritable> {
	  @Override
	  protected void map(Object key, Text line, Context ctx) throws InterruptedException, IOException {
		  double minQual = new Double(ctx.getConfiguration().get("minQuality")).doubleValue();
		  String[] tempVals = line.toString().split("\t");
		  String year = tempVals[0];
		  String month = tempVals[1];
		  int temperature = new Integer(tempVals[2]).intValue();
		  double quality = new Double(tempVals[3]).doubleValue();
		  
		  if (quality >= minQual) {
			  ctx.write(new Text(year + "\t" + month), new IntWritable(temperature));
		  }
	  }
  }
  
  public static class AvgTempPerMonthReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
	  @Override
	  protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws InterruptedException, IOException {
		  int sumTemp = 0;
		  float avgTemp = 0;
		  int count = 0;
		  
		  for (IntWritable value : values) {
			  sumTemp += value.get();
			  count++;
		  }
		  
		  avgTemp = (float) sumTemp / (float) count;
		  System.out.println(key.toString() + ": " + avgTemp);
		  ctx.write(key, new FloatWritable(avgTemp));
	  }
  }
}
