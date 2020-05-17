package co.enydata.tutorials.dataflow.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.schemas.Schema;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class TableUtils {
    public static TableSchema getTableSchema(String schema, String header, String delimiter) {

        Map<String, String> schemaMap =(schema!=null)? getSchemaMap(schema,"",","): getSchemaMap(header,":String",delimiter);

        List<TableFieldSchema> fields =schemaMap.entrySet().stream().map(e->new TableFieldSchema().setName(e.getKey()).setType(e.getValue()))
                .collect(Collectors.toList());

        return new TableSchema().setFields(fields);
    }


    public static Map<String, String>  getSchemaMap(String schema,String value,String delimiter){
        return Arrays.stream(schema.split(delimiter))
                .map(s -> s.concat(value).split(":"))
                .collect(Collectors.toMap(s -> s[0], s -> FieldType.toBigQueryType(s[1])));
    }



    public static Schema getSchema(String header, String delimiter){

     List <Schema.Field>fields=  Arrays.stream(header.split(delimiter))
                .map(s -> Schema.Field.of(s, Schema.FieldType.STRING))
                .collect(Collectors.toList());

        return Schema.builder().addFields(fields).build();
    }



    public static List<String> getCastExpression(String schema){

        List <String>fields=  Arrays.stream(schema.split(","))
                .map(s -> s.split(":"))
                .collect(Collectors.toMap(s -> s[0], s ->String.format("cast('%s' AS %s)",s[0],s[1].toUpperCase())))
                .values()
                .stream().collect(Collectors.toList());
               // .collect(Collectors.toMap(s -> s[0],s->String.format("cast ( %s as %s) as %s",s[0],s[1],s[0])));


        return fields;
    }

}
