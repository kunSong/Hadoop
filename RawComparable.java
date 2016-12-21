package com.songkun.writabledemo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text.Comparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TextPair implements WritableComparable<TextPair>{
  private Text first;
  private Text second;
  
  public TextPair() {
  }
  
  public TextPair(String first, String second){
    set(new Text(first), new Text(second));
  }
  
  public TextPair(Text first, Text second){
    this.first = first;
    this.second = second;
  }
  
  public void set(Text first, Text second){
    this.first = first;
    this.second = second;
  }
  
  public Text getFirst() {
    return first;
  }

  public Text getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }
  
  @Override
  public int hashCode() {
    return this.first.hashCode() * 163 + this.second.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TextPair){
      TextPair o = (TextPair) obj;
      return this.first.equals(o.first) && this.second.equals(o.second);
    }
    return false;
  }

  @Override
  public int compareTo(TextPair o) {
    int cmp = this.first.compareTo(o.first);
    if(cmp != 0){
      return cmp;
    }
    return cmp = this.second.compareTo(o.second);
  }
  
  public static class Comparator extends WritableComparator {
    private static Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    public Comparator(){
      super(TextPair.class);
    }
    
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try {
        /** decodeVIntSize 返回字节数组中某字节长度，readVInt 返回该字节本身int */
        int firstT1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstT2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        int cmp = TEXT_COMPARATOR.compare(b1, s1, firstT1, b2, s2, firstT2);
        if(cmp != 0){
          return cmp;
        }
        return TEXT_COMPARATOR.compare(b1, s1 + firstT1, l1 - firstT1,
                b2, s2 + firstT2, l2 - firstT2);        
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
  
  static {
    /** Register an optimized comparator for a WritableComparable
      * implementation. Comparators registered with this method must be
      * thread-safe.
      */
    WritableComparator.define(TextPair.class, new Comparator());
  }
}
