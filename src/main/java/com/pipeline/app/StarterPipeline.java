/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pipeline.app;



import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
  public interface  Options extends PipelineOptions{
    
    @Description("Path to sales data file")
    String getInputSalesPath();
    void setInputSalesPath(String inputSalesPath);

    @Description("Path to user data file")
    String getInputUsersPath();
    void setInputUsersPath(String inputUsersPath);

    @Description("Path to output file")
    String getOutputPathString();
    void setOutputPathString(String outputPath);
  }
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    runPipeline(options);
  }
  
  public static class parseToSales extends DoFn<String, Sales>{
    @ProcessElement
    public void ProcessElement(@Element String line, OutputReceiver<Sales> out){
      try {
          Gson gson=new Gson();
          Sales sale = gson.fromJson(line, Sales.class);
          out.output(sale);
      } catch (JsonParseException e) {
        e.printStackTrace();
      }
    }
  }
  public static class parseToUser extends DoFn<String, UserData>{
    @ProcessElement
    public void ProcessElement(@Element String line, OutputReceiver<UserData> out){
      try {
          Gson gson=new Gson();
          UserData user = gson.fromJson(line, UserData.class);
          System.out.println(user);
          out.output(user);
      } catch (JsonParseException e) {
        e.printStackTrace();
      }
    }
  }

  private static final Schema locSalesSchema = Schema
    .builder()
    .addStringField("address")
    .addNullableField("sales", FieldType.INT32)
    .build() ;

  private static void runPipeline(Options options){
    Pipeline pipeline = Pipeline.create(options);
    options.setJobName("dataProcessing");

    final PCollectionView<Map<Integer,Integer>> userIdSalesView=  pipeline.apply("ReadJSONSales", TextIO.read().from(options.getInputSalesPath()))
      .apply("parseStringToSales", ParDo.of(new parseToSales()))
      .apply("Group UserSales", Group.<Sales>byFieldNames("user_id")
                                      .aggregateField("Quantity", Sum.ofIntegers(), "ItemsBuy"))
      .apply("Flatten",Select.fieldNames("key.user_id","value.ItemsBuy"))
      .apply("toString",ParDo.of(new DoFn<Row, String>(){
        @DoFn.ProcessElement
        public void ProcessElement(@DoFn.Element Row row, OutputReceiver<String> out){
          String line = "";
          line+=row.getInt32("user_id")+","+row.getInt32("ItemsBuy");
          out.output(line);
        }
      }))
      .apply("keyedValues", MapElements
        .via(new SimpleFunction<String, KV<Integer, Integer>>(){
          @Override
          public KV<Integer,Integer> apply(String line){
              String[] ls = line.split(",");
            return KV.of( Integer.valueOf(ls[0]), Integer.valueOf(ls[1]));
          }
        }))
      .apply("makeView",View.asMap());

    PCollection<UserData> userData = pipeline.apply("ReadJsonUsers",TextIO.read().from(options.getInputUsersPath()))
      .apply("parseToUser",ParDo.of(new parseToUser()));
    
    userData
      .apply("sideInput",ParDo.of(new DoFn<UserData, Row>(){
        @DoFn.ProcessElement
        public void ProcessElement(ProcessContext context, OutputReceiver<Row> out){
          
          Map<Integer,Integer> userSales = context.sideInput(userIdSalesView); //side input

          UserData user = context.element();
          if(userSales.containsKey(user.user_id)){
            Row outputLine = Row
              .withSchema(locSalesSchema)
              .addValues(user.user_address.addressToString(),userSales.get(user.user_id))
              .build();
            out.output(outputLine);
          }else{
            Row outputLine = Row
              .withSchema(locSalesSchema)
              .addValues(user.user_address.addressToString(),0)
              .build();
            out.output(outputLine);
          }
        }
      }).withSideInputs(userIdSalesView)).setRowSchema(locSalesSchema)
      .apply("GroupByAddress",Group.<Row>byFieldNames("address")
                                  .aggregateField("sales", Sum.ofIntegers(), "sales"))
      .apply("Flatten",Select.fieldNames("key.address","value.sales"))
      .apply("toString",ParDo.of(new DoFn<Row, String>(){
        @DoFn.ProcessElement
        public void ProcessElement(@DoFn.Element Row row, OutputReceiver<String> out){
          String line = "";
          line+=row.getString("address")+"->"+row.getInt32("sales");
          out.output(line);
        }
      }))
      .apply("writeToDest",TextIO.write().to(options.getOutputPathString()+"/CityWiseSales").withSuffix(".txt").withoutSharding());
    pipeline.run().waitUntilFinish();
  }
}
