package co.enydata.tutorials.dataflow.transformer;

import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import co.enydata.tutorials.dataflow.util.SchemaUtil;
import co.enydata.tutorials.dataflow.util.TableUtils;
import com.google.api.client.util.DateTime;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class BasicTransformer {

    public static PCollection<Row> transform(PCollection<Row> rowPCollection, SchemaDataInfo schemaDataInfo){

        Schema schema= SchemaUtil.getSchema(schemaDataInfo,true);

        PCollection<Row> pCollection=   rowPCollection
                 .apply("COMPUTE NEW FIELD", SqlTransform.query(
                String.format("SELECT %s,%s from PCOLLECTION ",castField(schemaDataInfo),computedField(schemaDataInfo))))
                .setRowSchema(schema);

         pCollection.apply(logRecords("T"));

       /*    PCollection<TableRow> toto= pCollection.apply(ParDo.of(new DoFn<Row, TableRow>() {

                  @ProcessElement
                  public void processElement(ProcessContext c){
                    //  System.out.println(c.element().getValues());
                      c.output(BigQueryUtils.toTableRow(c.element()).set("ingestDate", "2222"));
                  }

              }));


           toto.apply(logTableRow(""));*/
           return pCollection;
}





    private static String computedField(SchemaDataInfo schemaDataInfo){
        return  String.join(",",schemaDataInfo.getComputedfields().stream()
                .map(field ->field.getSql() +" as "+field.getName())
                .collect(Collectors.toList()));
    }

    private static String castField(SchemaDataInfo schemaDataInfo){

       return String.join(",",SchemaUtil.getSchema(schemaDataInfo,false)
                .getFields()
                .stream()
                .map(field -> "cast("+field.getName()+" as "+field.getType().getTypeName().name()+")")
                .collect(Collectors.toList()));
    }

    private static MapElements<Row, Void> logRecords(String suffix) {
        return MapElements.via(
                new SimpleFunction<Row, Void>() {
                    @Override
                    public Void apply(Row input) {
                        System.out.println(input.getValues() + suffix);
                        System.out.println(input.getSchema().getFields());
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
                        System.out.println(input.keySet());;
                        return null;
                    }
                });
    }
}
