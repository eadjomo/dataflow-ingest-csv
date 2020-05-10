package co.enydata.tutorials.dataflow;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public  class  CsvParser extends DoFn<String, TableRow> {
    private static final Logger logger = LoggerFactory.getLogger(CsvParser.class);

    @ProcessElement
    public void process(ProcessContext c) {

        IngestCSVOptions ops = c.getPipelineOptions().as(IngestCSVOptions.class);

        logger.info("Input File has this header {}", ops.getHeader());

        System.out.println(c.element());
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




}
