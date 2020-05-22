package co.enydata.tutorials.dataflow.transformer;

import co.enydata.tutorials.dataflow.model.SchemaDataInfo;
import co.enydata.tutorials.dataflow.util.SchemaUtil;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.stream.Collectors;

public class BasicTransformer  {

    public static PCollection<Row> transform(PCollection<Row> rowPCollection, SchemaDataInfo schemaDataInfo){

        Schema schema= SchemaUtil.getSchema(schemaDataInfo,false);

      return  rowPCollection.apply("TRANSFORM",ParDo.of(new DoFn<Row, Row>() {

                  @ProcessElement
                  public void processElement(ProcessContext c){
                      c.output(SchemaUtil.castCellValue(c.element(),schema));
                  }

              })).setRowSchema(schema);
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
