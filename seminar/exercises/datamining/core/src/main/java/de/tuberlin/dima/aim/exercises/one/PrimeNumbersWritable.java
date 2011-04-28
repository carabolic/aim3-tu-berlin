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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

public class PrimeNumbersWritable implements Writable {

  private int[] numbers;
  
  public PrimeNumbersWritable() {
    numbers = new int[0];
  }

  public PrimeNumbersWritable(int... numbers) {
    this.numbers = numbers;
  }

  @Override
  public void write(DataOutput out) throws IOException {
	  for (int number : numbers) {
		  out.writeInt(number);
	  }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
	  System.out.println(in.toString());
	  Vector<Integer> buffer = new Vector<Integer>();
	  
	  int input;
	  try {
		  while ((input = in.readInt()) > 0) {
			  buffer.add(new Integer(input));
			  System.out.println("Added " + input + " size is " + buffer.size());
		  }
	  }
	  catch(EOFException e) {
		  int[] numbers = new int[buffer.size()];
		  System.out.println("Numbers length: " + numbers.length);
		  for (int n = 0; n < buffer.size(); n++) {
			  numbers[n] = buffer.elementAt(n).intValue();
		  }
	  }
	  
	  System.out.println("Numbers:");
	  System.out.println("Lenght: " + numbers.length);
	  for (int number : numbers) {
		  System.out.println(number);
	  }
	  
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PrimeNumbersWritable) {
      PrimeNumbersWritable other = (PrimeNumbersWritable) obj;
      return Arrays.equals(numbers, other.numbers);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(numbers);
  }
}
