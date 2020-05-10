package co.enydata.tutorials.dataflow.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

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


    private static Map<String, String>  getSchemaMap(String schema,String value,String delimiter){
        return Arrays.stream(schema.split(delimiter))
                .map(s -> s.concat(value).split(":"))
                .collect(Collectors.toMap(s -> s[0], s -> FieldType.toBigQueryType(s[1])));
    }

}
