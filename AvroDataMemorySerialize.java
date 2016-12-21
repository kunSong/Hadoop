package com.songkun.avrodemo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.IntWritable;

public class AvroStringPair {
  
  public static void main(String[] args) {
    String json = "{" +
        "\"name\":\"L\"" +
        "\"name\":\"R\"" +
        "}";
    
    serializeAvro(json, null);
  }
  
  public static byte[] serializeAvro (String json, String schemaStr){
    try {
      /** 通过两种方式 schemaStr 解析 和 从 .avsc 文件解析 schema */
      Schema schema = new Schema.Parser().parse(new File(".StringPari.avsc"));
      //Schema schema = new Schema.Parser().parse(schemaStr);
      
      /** 通过两种方式来获取 avro 值，
       *  方法一 new GenericData.Record */
      GenericRecord datum1 = new GenericData.Record(schema);
      datum1.put("left", new Utf8("L"));
      datum1.put("right", new Utf8("R"));
      
      ByteArrayOutputStream out1 = new ByteArrayOutputStream();
      GenericDatumWriter<GenericRecord> writer1 = 
          new GenericDatumWriter<GenericRecord>();
      Encoder encoder1 = EncoderFactory.get().binaryEncoder(out1, null);
      writer1.write(datum1, encoder1);
      encoder1.flush();
      
      /** 再从流中读出 */
      GenericDatumReader<GenericRecord> reader1 = 
          new GenericDatumReader<GenericRecord>();
      Decoder decoder1 = DecoderFactory.get()
          .binaryDecoder(out1.toByteArray(), null);//@Deprecated createBinaryDecoder 
      GenericRecord result = reader1.read(null, decoder1);
      result.get("left");
      result.get("right");
      
      /** 方法二 从json读取数据 */
      InputStream in2 = new ByteArrayInputStream(json.getBytes());
      DataInputStream din2 = new DataInputStream(in2);
      GenericDatumReader<GenericRecord> reader2 = 
          new GenericDatumReader<GenericRecord>(schema);
      Decoder decoder2 = DecoderFactory.get().jsonDecoder(schema, din2);

      ByteArrayOutputStream out2 = new ByteArrayOutputStream();
      GenericDatumWriter<GenericRecord> writer2 = 
          new GenericDatumWriter<GenericRecord>(schema);
      Encoder encoder2 = EncoderFactory.get().binaryEncoder(out2, null);
      GenericRecord datum2 = null;
      
      while(true){
        try { 
          datum2 = reader2.read(null, decoder2);
        } catch (EOFException eof) {
          break;
        }
        writer2.write(datum2, encoder2);
      }
      encoder2.flush();
      
      /** 方法三 使用SpecificDatumWritable 使用get(),set()方法*/
      StringPair datum3 = new StringPair();
      datum3.setLeft("L");
      datum3.setRight("R");
      ByteArrayOutputStream out3 = new ByteArrayOutputStream();
      DatumWriter<StringPair> writer3 = 
          new SpecificDatumWriter<StringPair>(StringPair.class);
      Encoder encoder3 = EncoderFactory.get().binaryEncoder(out3, null);
      writer3.write(datum3, encoder3);
      encoder3.flush();
      
      /** 再从流中读出*/
      DatumReader<StringPair> reader3 = 
          new SpecificDatumReader<StringPair>(StringPair.class);
      Decoder decoder3 = DecoderFactory.get()
          .binaryDecoder(out1.toByteArray(), null);
      StringPair result3 = reader3.read(null, decoder3);
      result3.getLeft();
      result3.getRigtt();
      
    } catch (Exception e) {
      try {
        //close();
      } catch (Exception exception){
      }
    }
    //return out1.toByteArray();
    //return out2.toByteArray();
    return null;
  }
  
  public static class StringPair{
    private String left;
    private String right;
    
    public String getLeft() {
      return left;
    }
    public void setLeft(String left) {
      this.left = left;
    }
    public String getRight() {
      return right;
    }
    public void setRight(String right) {
      this.right = right;
    }
  }
}

