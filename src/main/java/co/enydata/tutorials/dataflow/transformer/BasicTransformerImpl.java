package co.enydata.tutorials.dataflow.transformer;

import co.enydata.tutorials.dataflow.common.IngestCSVOptions;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import javafx.scene.control.Tab;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.beam.vendor.grpc.v1p26p0.org.bouncycastle.asn1.x500.style.RFC4519Style.c;

public class BasicTransformerImpl implements ITransformer {

    private static final Logger logger = LoggerFactory.getLogger(BasicTransformerImpl.class);

   /* @Override
    public PCollection<TableRow> transform(PCollection<TableRow> tableRowPCollection) {
         PCollection<TableRow> output= tableRowPCollection.apply("TRANSFORM",
                 ParDo.of(new DoFn<TableRow, TableRow>() {

                     @ProcessElement
                     public void processElement(ProcessContext c) {

                     c.element().set("ingestDate", SqlTransform.query(
                             "SELECT CURRENT_TIMESTAMP() as now"));

                 }));
        return output;
    }
}*/



    @Override
    public  PCollection<TableRow> transform(PCollection<Row> rowPCollection,List<String> fieldList) {

        PCollection<TableRow> output= BasicTransformer.transform(rowPCollection,fieldList);

        return output;

    }


}
