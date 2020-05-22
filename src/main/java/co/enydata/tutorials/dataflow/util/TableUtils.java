package co.enydata.tutorials.dataflow.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class TableUtils {

   /*
    public static TableSchema getTableSchema(String schema, String header, String delimiter) {

        Map<String, String> schemaMap =(schema!=null)? getSchemaMap(schema,"",","): getSchemaMap(header,":String",delimiter);
        //System.out.println(schemaMap);
        List<TableFieldSchema> fields =schemaMap.entrySet().stream().map(e->new TableFieldSchema().setName(e.getKey()).setType(e.getValue()))
                .collect(Collectors.toList());
        //System.out.println(fields);

        return new TableSchema().setFields(fields);
    }


    public static Map<String, String>  getSchemaMap(String schema,String value,String delimiter){

        Map<String, String> result=new HashMap<String,String>();
        String []s=schema.split(delimiter);
        for (int i = 0; i < s.length; i++) {
            String [] str=s[i].concat(value).split(":");
            result.put(str[0],FieldType.toBigQueryType(s[1]));
        }


        return result;

        *//*return Arrays.stream(schema.split(delimiter))
                .map(s -> s.concat(value).split(":"))

                .collect(Collectors.toMap(s -> s[0], s -> FieldType.toBigQueryType(s[1])));*//*
    }



    public static Schema getSchema(String header, String delimiter){

     List <Schema.Field>fields=  Arrays.stream(header.split(delimiter))
                .map(s -> Schema.Field.of(s, Schema.FieldType.STRING))
                .collect(Collectors.toList());

        return Schema.builder().addFields(fields).build();
    }



    public static List<String> getCastExpression(String schema){

        List <String>fields=  Arrays.stream(schema.split(","))
                .map(s -> String.format("cast(%s AS %s)",s.split(":")[0],FieldType.toBigQueryType(s.split(":")[1])))
               // .collect(Collectors.toMap(s -> s[0], ))
              //  .values()
             //   .stream()
                .collect(Collectors.toList());
               // .collect(Collectors.toMap(s -> s[0],s->String.format("cast ( %s as %s) as %s",s[0],s[1],s[0])));


        return fields;
    }
*/
}
