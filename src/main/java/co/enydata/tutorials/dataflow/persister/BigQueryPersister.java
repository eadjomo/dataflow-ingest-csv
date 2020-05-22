package co.enydata.tutorials.dataflow.persister;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;

public class BigQueryPersister {

    public static WriteResult persist(PCollection<Row> rowPCollection, SchemaDataInfo schemaDataInfo) {
        IngestCSVOptions options=rowPCollection.getPipeline().getOptions().as(IngestCSVOptions.class);

        return  rowPCollection.apply("BEAM ROW TO TABLEROW",ParDo.of(new DoFn<Row, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                c.output(BigQueryUtils.toTableRow(c.element()));
            }

        })).apply("BIGQUERY PERSIST", BigQueryIO.writeTableRows()
                .to(options.getTableName())
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_TRUNCATE)
                .withSchema(BigQueryUtils.toTableSchema(rowPCollection.getSchema())));
    }
}
