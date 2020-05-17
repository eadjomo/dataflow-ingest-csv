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
import co.enydata.tutorials.dataflow.reader.IReader;
import co.enydata.tutorials.dataflow.reader.csv.CsvParser;
import co.enydata.tutorials.dataflow.reader.csv.CsvReaderImpl;
import co.enydata.tutorials.dataflow.transformer.BasicTransformerImpl;
import co.enydata.tutorials.dataflow.transformer.ITransformer;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

/** Tests of WordCount. */
@RunWith(JUnit4.class)
public class IngestCsvToBigQueryTest {





  public  IngestCSVOptions options = PipelineOptionsFactory.as(IngestCSVOptions.class);

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.fromOptions(options);

  private static MapElements<Row, Void> logRecords(String suffix) {
    return MapElements.via(
            new SimpleFunction<Row, Void>() {
              @Override
              public Void apply(Row input) {
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
    options.setSchema("year:INTEGER,month:INTEGER,day:INTEGER,wikimedia_project:VARCHAR,language:VARCHAR,title:VARCHAR,views:INTEGER");

  }



  @Test
  public void test_Reader() throws Exception {


    IReader reader=new CsvReaderImpl();

    PCollection<Row> readerOutPut=reader.read(testPipeline,options);

    String[] split = "2018,8,13,Wikinews,English,Spanish football: Sevilla signs Aleix Vidal from FC Barcelona,12331".split(",");
    Row row= Row.withSchema(TableUtils.getSchema(options.getHeader(),options.getDelimiter()))
            .addValues(split).build();


   readerOutPut.apply(logRecords(" "));

    List<Row>expectedResult= new ArrayList<Row>();
    expectedResult.add(row);

    // Assert that the output PCollection matches
    PAssert.that(readerOutPut).containsInAnyOrder(expectedResult);

    // Run the pipeline.
    testPipeline.run().waitUntilFinish();


  }


  @Test
  public void test_Transformer() throws Exception {


    String[] split = "2018,8,13,Wikinews,English,Spanish football: Sevilla signs Aleix Vidal from FC Barcelona,12331".split(",");
    Row row= Row.withSchema(TableUtils.getSchema(options.getHeader(),options.getDelimiter()))
            .addValues(split).build();



    PCollection<Row> rows=TestPipeline.fromOptions(options)
            .apply("Create input",Create
                    .of(row).withRowSchema(TableUtils.getSchema(options.getHeader(),options.getDelimiter())));

    System.out.println(TableUtils.getSchema(options.getHeader(),options.getDelimiter()));

    ITransformer transformer=new BasicTransformerImpl();

    PCollection<TableRow> transformerOutPut=transformer.transform(rows,TableUtils.getCastExpression(options.getSchema()));



    TableRow tableRow = new TableRow();
    for (int i = 0; i < split.length; i++) {
      TableFieldSchema col = TableUtils.getTableSchema(options.getSchema(),options.getHeader(),",").getFields().get(i);
      tableRow.set(col.getName(), split[i]);
    }

    List<TableRow>expectedResult= new ArrayList<TableRow>();
    expectedResult.add(tableRow);

     transformerOutPut.apply(logTableRow(" "));

    // Assert that the output PCollection matches
    PAssert.that(transformerOutPut).containsInAnyOrder(expectedResult);

    // Run the pipeline.
    testPipeline.run().waitUntilFinish();

  }


/*
  @Test
  public void test_parse_CSV_format_successfully_with_tablerow() throws Exception {

    List<String> input = new ArrayList<>();

    String header="year,month,day,wikimedia_project,language,title,views";



    IngestCSVOptions options = PipelineOptionsFactory.as(IngestCSVOptions.class);
    options.setDelimiter(",");
    options.setHeader(header);
    options.setSchema(null);


    input.add("2018,8,13,Wikinews,English,Spanish football: Sevilla signs Aleix Vidal from FC Barcelona,12331");

    PCollection<TableRow> output= TestPipeline.fromOptions(options)
            .apply("Create input", Create.of(input))
            .apply("Parse pipeline",
                    ParDo.of(new CsvParser()));

    TableRow row = new TableRow();
    String[] split = "2018,8,13,Wikinews,English,Spanish football: Sevilla signs Aleix Vidal from FC Barcelona,12331".split(",");
    for (int i = 0; i < split.length; i++) {
      TableFieldSchema col = TableUtils.getTableSchema(null,header,",").getFields().get(i);
      row.set(col.getName(), split[i]);
    }

    List<TableRow>expectedResult= new ArrayList<TableRow>();
    expectedResult.add(row);

    // Run the pipeline.
    testPipeline.run();

    // Assert that the output PCollection matches
    PAssert.that(output).containsInAnyOrder(expectedResult);

  }*/

  @Test
  public void test_parse_header_return_empty_list() throws Exception {

  }

}
