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
package co.enydata.tutorials.dataflow;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import co.enydata.tutorials.dataflow.reader.IReader;
import co.enydata.tutorials.dataflow.reader.csv.CsvReaderImpl;
import co.enydata.tutorials.dataflow.transformer.BasicTransformerImpl;
import co.enydata.tutorials.dataflow.transformer.ITransformer;
import co.enydata.tutorials.dataflow.util.SchemaUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Tests of WordCount. */
@RunWith(JUnit4.class)
public class IngestCsvToBigQueryTest {





  public  IngestCSVOptions options = PipelineOptionsFactory.as(IngestCSVOptions.class);
  ObjectMapper mapper = new ObjectMapper();

  SchemaDataInfo schemaDataInfo;

  Schema schemaWithoutCompute;
  Schema schemaWithCompute;
  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.fromOptions(options);

  private static MapElements<Row, Void> logRecords(String suffix) {
    return MapElements.via(
            new SimpleFunction<Row, Void>() {
              @Override
              public Void apply(Row input) {
                System.out.println(input.getSchema().getFields());
                System.out.println(input.getValues() + suffix);
                return null;
              }
            });
  }

  private static MapElements<TableRow, Void> logTableRow(String suffix) {
    return MapElements.via(
            new SimpleFunction<TableRow, Void>() {
              @Override
              public Void apply(TableRow input) {
                System.out.println(input.keySet());
                System.out.println(input.values()+ suffix);

                return null;
              }
            });
  }


  @Before
  public void setUp() throws Exception {
    String header="year,month,day,wikimedia_project,language,title,views";
    options.setInputFile(this.getClass().getClassLoader().getResource("sample2.csv").getFile());
    options.setDelimiter(",");
    options.setHeader(header);
    //options.setSchema("year:STRING,month:STRING,day:STRING,wikimedia_project:STRING,language:STRING,title:STRING,views:STRING");
    options.setSchema(this.getClass().getClassLoader().getResource("schema2.json").getFile());
    schemaDataInfo=mapper.readValue(new File(options.getSchema()),SchemaDataInfo.class);
    schemaWithCompute=SchemaUtil.getSchema(schemaDataInfo,true);
    schemaWithoutCompute=SchemaUtil.getSchema(schemaDataInfo,false);
    System.out.println(schemaDataInfo);
   // System.out.println(schemaWithoutCompute);
   // System.out.println(schemaWithCompute);
  }



  @Test
  public void test_Reader() throws Exception {


    IReader reader=new CsvReaderImpl();

    PCollection<Row> readerOutPut=reader.read(testPipeline,schemaDataInfo,options);

    String[] split = "2018,8,13,Wikinews,English,Spanish football: Sevilla signs Aleix Vidal from FC Barcelona,12331".split(",");
    Row row= Row.withSchema(Schema.builder().addFields(schemaDataInfo.getFields().stream()
            .map(s -> Schema.Field.of(s.getName(), Schema.FieldType.STRING))
            .collect(Collectors.toList())).build())
            .addValues(split).build();

      List<Row>expectedResult= new ArrayList<Row>();
      expectedResult.add(row);

    // Assert that the output PCollection matches
    PAssert.that(readerOutPut).containsInAnyOrder(expectedResult);

    // Run the pipeline.
    testPipeline.run().waitUntilFinish();


  }


  @Test
  public void test_Transformer() throws Exception {

      IReader reader=new CsvReaderImpl();

      PCollection<Row> readerOutPut=reader.read(testPipeline,schemaDataInfo,options);

      //readerOutPut.apply(logRecords("e"));

    String[] split = "2018,8,13,Wikinews,English,Spanish football: Sevilla signs Aleix Vidal from FC Barcelona,12331"
            .split(",");
    Row row= Row.withSchema(Schema.builder().addFields(schemaDataInfo.getFields().stream()
            .map(s -> Schema.Field.of(s.getName(), Schema.FieldType.STRING))
            .collect(Collectors.toList())).build())
            .addValues(split).build();

    ITransformer transformer=new BasicTransformerImpl();

    PCollection<Row> transformerOutPut=transformer.transform(readerOutPut,schemaDataInfo);

    List<Row>expectedResult= new ArrayList<Row>();
    expectedResult.add(row);

   transformerOutPut.apply(logRecords(" "));

    // Assert that the output PCollection matches
    PAssert.that(transformerOutPut).containsInAnyOrder(expectedResult);

    // Run the pipeline.
    testPipeline.run().waitUntilFinish();

  }



  @Test
  public void test_parse_header_return_empty_list() throws Exception {

  }

}
