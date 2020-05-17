package co.enydata.tutorials.dataflow.transformer;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.List;

public class BasicTransformer {
    public static PCollection<TableRow> transform(PCollection<Row> rowPCollection, List<String> fieldList){
      return   rowPCollection.apply("TRANSFORM", SqlTransform.query(
                String.format("SELECT %s,CURRENT_TIMESTAMP AS ingestDate from PCOLLECTION",String.join(",",fieldList))))
                .apply(ParDo.of(new DoFn<Row, TableRow>() {

                    @ProcessElement
                    public void processElement(ProcessContext c){

                        IngestCSVOptions options = c.getPipelineOptions().as(IngestCSVOptions.class);
                        Row row=c.element();
                        TableRow tableRow = new TableRow();
                        for (int i = 0; i < row.getSchema().getFields().size(); i++) {
                            TableFieldSchema col = TableUtils.getTableSchema(options.getSchema(),options.getHeader(),",").getFields().get(i);
                            tableRow.set(col.getName(), row.getValue(col.getName()));
                        }
                        c.output(tableRow);
                    }
                }));
    }
}
