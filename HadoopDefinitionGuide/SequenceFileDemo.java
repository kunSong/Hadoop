package com.songkun.sequencefile;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileDemo {
  private static final String[] DATA = { 
      "One, two, buckle my shoe", 
      "Three, four, shut the door",
      "Five, six, pick up sticks", 
      "Seven, eight, lay them straight", 
      "Nine, ten, a big fat han" };
  private static final String DEMO_URI = 
      "hdfs://localhost:9000/user/SequenceFile.seq";

  private void writeToSequenceFile() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(DEMO_URI), conf);
    Path name = new Path(DEMO_URI);
    IntWritable key = new IntWritable();
    Text value = new Text();

    SequenceFile.Writer writer = SequenceFile.createWriter(
        fs, 
        conf, 
        name, 
        key.getClass(), 
        value.getClass());

    for (int i = 0; i < 100; i++) {
      key.set(100 - i);
      value.set(DATA[i % DATA.length]);
      writer.append(key, value);
    }
    writer.close();
    fs.close();
  }

  private void readFromSequenceFile() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(DEMO_URI), conf);
    Path path = new Path(DEMO_URI);
    
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
    Writable key = (Writable) 
        ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    Writable value = (Writable) 
        ReflectionUtils.newInstance(reader.getValueClass(), conf);
    long position = reader.getPosition();
    while(reader.next(key, value)){
      String syncScreen = reader.syncSeen() ? "*" : "";
      System.out.printf("[%s%s]\t%s\t%s\n", position, syncScreen, key, value);
      position = reader.getPosition();
    }
  }

  public static void main(String[] args) {
    SequenceFileDemo sequenceFile = new SequenceFileDemo();
    try {
      sequenceFile.writeToSequenceFile();
      sequenceFile.readFromSequenceFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

/** output
[128]   100 One, two, buckle my shoe
[173]   99  Three, four, shut the door
[220]   98  Five, six, pick up sticks
[264]   97  Seven, eight, lay them straight
[314]   96  Nine, ten, a big fat han
[359]   95  One, two, buckle my shoe
[404]   94  Three, four, shut the door
[451]   93  Five, six, pick up sticks
[495]   92  Seven, eight, lay them straight
[545]   91  Nine, ten, a big fat han
...
[1931]  61  Nine, ten, a big fat han
[1976]  60  One, two, buckle my shoe
[2021*] 59  Three, four, shut the door
**/



