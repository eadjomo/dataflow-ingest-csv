package co.enydata.tutorials.dataflow.model;

import java.util.List;

public class SchemaDataInfo {

    private List<FieldInfo> fields;
    private List<FieldInfo>computedfields;

    public List<FieldInfo> getFields() {
        return fields;
    }

    public void setFields(List<FieldInfo> fields) {
        this.fields = fields;
    }

    public List<FieldInfo> getComputedfields() {
        return computedfields;
    }

    public void setComputedfields(List<FieldInfo> computedfields) {
        this.computedfields = computedfields;
    }

    @Override
    public String toString() {
        return "SchemaDataInfo{" +
                "fields=" + fields +
                ", computedfields=" + computedfields +
                '}';
    }
}
