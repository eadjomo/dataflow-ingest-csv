package co.enydata.tutorials.dataflow.reader.csv;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public  class  CsvParser {
    private static final Logger logger = LoggerFactory.getLogger(CsvParser.class);


  /*  public PCollection<TableRow> parse(PCollection<String> input) {


        return input.apply(ParDo.of(new DoFn<String, TableRow>() {
            @ProcessElement
            public void process(ProcessContext c){

                try {
                    IngestCSVOptions ops = c.getPipelineOptions().as(IngestCSVOptions.class);

                    logger.info("Input File has this header {}", ops.getHeader());

                    if (c.element().equalsIgnoreCase(ops.getHeader())) return;
                    String[] split = c.element().split(ops.getDelimiter());
                    if (split.length > ops.getHeader().split(ops.getDelimiter()).length) return;
                    TableRow row = new TableRow();
                    for (int i = 0; i < split.length; i++) {
                        TableFieldSchema col = TableUtils.getTableSchema(ops.getSchema(),ops.getHeader(),ops.getDelimiter()).getFields().get(i);
                        row.set(col.getName(), split[i]);
                    }
                    c.output(row);
                } catch (Exception e) {
                    logger.error(e.getLocalizedMessage());
                    throw e;
                }
            }
        }));
    }
*/

    public static PCollection<Row> parse(PCollection<String> input) {


        return input.apply(ParDo.of(new DoFn<String, Row>() {
            @ProcessElement
            public void process(ProcessContext c){

                try {
                    IngestCSVOptions ops = c.getPipelineOptions().as(IngestCSVOptions.class);

                    logger.info("Input File has this header {}", ops.getHeader());

                    if (c.element().equalsIgnoreCase(ops.getHeader())) return;
                    String[] split = c.element().split(ops.getDelimiter());
                    if (split.length > ops.getHeader().split(ops.getDelimiter()).length) return;
                   Row row= Row.withSchema(TableUtils.getSchema(ops.getHeader(),ops.getDelimiter()))
                            .addValues(split).build();

                    c.output(row);
                } catch (Exception e) {
                    logger.error(e.getLocalizedMessage());
                    throw e;
                }
            }
        }));
    }
}

    /*   @DoFn.ProcessElement
        public void process(DoFn.ProcessContext c) {

            IngestCSVOptions ops = c.getPipelineOptions().as(IngestCSVOptions.class);

            logger.info("Input File has this header {}", ops.getHeader());

            if (c.element().equalsIgnoreCase(ops.getHeader())) return;
            String[] split = c.element().split(ops.getDelimiter());
            if (split.length > ops.getHeader().split(ops.getDelimiter()).length) return;
            TableRow row = new TableRow();
            for (int i = 0; i < split.length; i++) {
                TableFieldSchema col = TableUtils.getTableSchema(ops.getSchema(),ops.getHeader(),ops.getDelimiter()).getFields().get(i);
                row.set(col.getName(), split[i]);
            }
            c.output(row);
        }
    }))


    }

    }

*/
