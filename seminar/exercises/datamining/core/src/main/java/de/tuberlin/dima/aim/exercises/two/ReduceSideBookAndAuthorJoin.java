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

package de.tuberlin.dima.aim.exercises.two;

import de.tuberlin.dima.aim.exercises.hadoop.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.ComparisonChain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class ReduceSideBookAndAuthorJoin extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job reduceSideJoin = new Job(new Configuration(getConf()), "ReduceSideJoin");

    reduceSideJoin.setMapOutputKeyClass(JoinKey.class);
    reduceSideJoin.setMapOutputValueClass(Text.class);
    reduceSideJoin.setReducerClass(JoinReducer.class);
    reduceSideJoin.setOutputKeyClass(Text.class);
    reduceSideJoin.setOutputValueClass(Text.class);
    reduceSideJoin.setOutputFormatClass(TextOutputFormat.class);
    
    reduceSideJoin.setPartitionerClass(JoinPartinioner.class);
    reduceSideJoin.setGroupingComparatorClass(JoinGroupingComparator.class);
    
    reduceSideJoin.getConfiguration().set("mapred.output.dir", outputPath.toString());
    
    MultipleInputs.addInputPath(reduceSideJoin, authors, TextInputFormat.class, ProjectAuthorsMapper.class);
    MultipleInputs.addInputPath(reduceSideJoin, books, TextInputFormat.class, ProjectBooksMapper.class);
    
    reduceSideJoin.waitForCompletion(true);
    
    return 0;
  }
  
  static class ProjectAuthorsMapper extends Mapper<Object, Text, JoinKey, Text> {
	 
	  @Override
	  protected void map(Object key, Text line, Context ctx) throws java.io.IOException ,InterruptedException {
		  String[] fields = line.toString().split("\t");
		  int id = Integer.parseInt(fields[0]);
		  String author = fields[1];
		  
		  ctx.write(new JoinKey(id, JoinKey.AUTHOR_TAG), new Text(author));
	  }
  }
  
  static class ProjectBooksMapper extends Mapper<Object, Text, JoinKey, Text> {
	  
	  @Override
	  protected void map(Object key, Text line, Context ctx) throws java.io.IOException ,InterruptedException {
		  String[] fields = line.toString().split("\t");
		  int id = Integer.parseInt(fields[0]);
		  int year = Integer.parseInt(fields[1]);
		  String title = fields[2];
		  
		  ctx.write(new JoinKey(id, JoinKey.BOOK_TAG), new Text(title + "\t" + year));
	  }
  }
  
  static class JoinPartinioner extends Partitioner<JoinKey, Text> {
	  
	  @Override
	  public int getPartition(JoinKey key, Text value, int numPartitions) {
		  return (key.getId() & Integer.MAX_VALUE) % numPartitions;
	  }
  }
  
  static class JoinGroupingComparator extends WritableComparator {

	protected JoinGroupingComparator() {
		super(JoinKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {
		JoinKey jK1 = (JoinKey) key1;
		JoinKey jK2 = (JoinKey) key2;
		
		return (new Integer(jK1.getId())).compareTo(new Integer(jK2.getId()));
	}
	  
  }
  
  static class JoinReducer extends Reducer<JoinKey, Text, Text, Text> {
	  
	  @Override
	  protected void reduce(JoinKey key, Iterable<Text> values, Context ctx) throws java.io.IOException ,InterruptedException {
		  //int id = key.getId();
		  boolean isFirst = true;
		  Text author = new Text("Unknown");
		  
		  for (Text value : values) {
			  if (isFirst) {
				  author = new Text(value);
				  isFirst = false;
			  }
			  else {
				  ctx.write(author, value);
			  }
		  }
	  }
  }
  
  static class JoinKey implements WritableComparable<JoinKey> {

	  private int id;
	  private int tag;
	  
	  public static final int AUTHOR_TAG = 0;
	  public static final int BOOK_TAG = 1;
	  public static final int NO_TAG = -1;
	  
	  public JoinKey() {
		  id = -1;
		  tag = NO_TAG;
	  }
	  
	  public JoinKey(int id, int tag) {
		  this.id = id;
		  this.tag = tag;
	  }
	  
	  public int getId() {
		  return id;
	  }
	  
	  public int getTag() {
		  return tag;
	  }	  
	  
	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		tag = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(tag);
	}

	@Override
	public int compareTo(JoinKey other) {
		return ComparisonChain.start().compare(getId(), other.getId()).compare(getTag(), other.getTag()).result();
	}
	
	@Override
	public String toString() {
		return id + "\t" + tag;
	}
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}
  }
}
