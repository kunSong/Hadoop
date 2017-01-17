package com.songkun.avrodemo;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

public class AvroDataFile {
  
  public static void main(String[] args) {
    try {
      Schema schema = new Schema.Parser().parse(
          new File("/home/songkun/StringPair.avsc"));
      File file = new File("/home/songkun/avroData.avro");
      GenericRecord datum = new GenericData.Record(schema);
      datum.put("left", new Utf8("song"));
      datum.put("right", new Utf8("kun"));
      datum.put("count", 2);
      datum.put("really", true);
      saveToAvroFile(schema, file, datum);
      readFromAvroFile(schema, file);
    
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public static void saveToAvroFile(
      Schema schema, File file, GenericRecord datum) throws IOException{
    GenericDatumWriter<GenericRecord> writer = 
        new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
        new DataFileWriter<GenericRecord>(writer);
    /** 创建文件 */
    dataFileWriter.create(schema, file);
    /** 如果提供的data不符合schema会抛出异常 */
    dataFileWriter.append(datum);
    dataFileWriter.close();
  }
  
  public static void readFromAvroFile(
      Schema schema, File file) throws IOException{
    GenericDatumReader<GenericRecord> reader =
        new GenericDatumReader<GenericRecord>(schema);
    /** dataFileReader seek() sync() 提供随机访问 */
    DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<GenericRecord>(file, reader);
    /** Method I datum 重用减少开销 */
    GenericRecord datum = null;
    while(dataFileReader.hasNext()){
      datum = dataFileReader.next(datum);
      System.out.println(datum.get("left").toString() +
                         datum.get("right").toString() +
                         datum.get("count").toString() +
                         datum.get("really").toString());
    }
    /** Method II */
    for(GenericRecord record : dataFileReader){
      //process record
    }
  }
}

