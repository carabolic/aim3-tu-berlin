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

import com.google.common.base.Joiner;
import de.tuberlin.dima.aim.exercises.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class SecondarySortBookSort extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job secondarySort = prepareJob(inputPath, outputPath, TextInputFormat.class, ByCenturyAndTitleMapper.class,
    		BookSortKey.class, Text.class, SecondarySortBookSortReducer.class, Text.class, NullWritable.class, TextOutputFormat.class);

    secondarySort.setPartitionerClass(SecondarySortPartioner.class);
    secondarySort.setGroupingComparatorClass(SecondarySortGroupComperator.class);
    
    secondarySort.waitForCompletion(true);
    return 0;
  }

  static class ByCenturyAndTitleMapper extends Mapper<Object,Text,BookSortKey,Text> {

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
    	String[] fields = line.toString().split("\t");
    	int century = Integer.parseInt(fields[1].substring(0, 2));
    	BookSortKey outKey = new BookSortKey(century, fields[2]);
    	ctx.write(outKey, new Text(fields[2]));
    }
  }
  
  /**
   * Custom Partitioner that partitions the data only by part of the key, which is the <code>century</code>
   * @author Christoph Bruecke
   *
   */
  static class SecondarySortPartioner extends Partitioner<BookSortKey, Text> {

	@Override
	public int getPartition(BookSortKey key, Text value, int partNum) {
		return (new Integer(key.century)).hashCode() % partNum;
	}
	  
  }
  
  /**
   * Custom GroupingComparator that groups the the data only on part of the keys, which is the
   * <code>century</code>
   * @author Christoph Bruecke
   *
   */
  static class SecondarySortGroupComperator extends WritableComparator {

	protected SecondarySortGroupComperator() {
		super(BookSortKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {
		BookSortKey bSk1 = (BookSortKey) key1;
		BookSortKey bSk2 = (BookSortKey) key2;
		
		return (new Integer(bSk1.century)).compareTo(new Integer(bSk2.century));
	}
	  
  }

  static class SecondarySortBookSortReducer extends Reducer<BookSortKey,Text,Text,NullWritable> {
    @Override
    protected void reduce(BookSortKey bookSortKey, Iterable<Text> values, Context ctx)
        throws IOException, InterruptedException {
      for (Text value : values) {
        String out = Joiner.on('\t').skipNulls().join(new Object[] { bookSortKey.toString(), value.toString() });
        ctx.write(new Text(out), NullWritable.get());
      }
    }
  }

  static class BookSortKey implements WritableComparable<BookSortKey> {
	  
	  private int century;
	  private String title;
	  
	  public int getCentury() {
		  return century; 
	  }
	  
	  public String getTitle() {
		  return title;
	  }
	  
    public BookSortKey() {
    	this.century = 0;
    	this.title = "";
    }
    
    public BookSortKey(int century, String title) {
    	this.century = century;
    	this.title = title;
    }

    @Override
    public int compareTo(BookSortKey other) {
      if (other != null) {
    	  if (century == other.century) {
    		  return title.compareTo(other.title);
    	  }
    	  else {
    		  return (new Integer(century)).compareTo(new Integer (other.century));
    	  }
      }
      else {
    	  return 1;
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeInt(century);
    	out.writeInt(title.length());
    	out.writeChars(title);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	century = in.readInt();
    	int strLen = in.readInt();
    	
    	StringBuilder strBld = new StringBuilder(strLen);
    	for (int i = 0; i < strLen; i++) {
    		strBld.append(in.readChar());
    	}
    	
    	title = strBld.toString();
    }

    @Override
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	
    	if (o instanceof BookSortKey) {
    		BookSortKey that = (BookSortKey) o;
    		if (that.title.equals(title) && that.century == century) {
    			return true;
    		}
    		else {
    			return false;
    		}
    	}
    	else {
    		return false;
    	}
    }

    @Override
    public int hashCode() {
    	return toString().hashCode();
    }    
    
    @Override
    public String toString() {
    	return century + "\t" + title;
    }
  }

}
