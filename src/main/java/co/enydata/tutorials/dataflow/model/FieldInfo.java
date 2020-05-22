package co.enydata.tutorials.dataflow.model;

public class FieldInfo {
    private String name;
    private String type;
    private String sql;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }


    @Override
    public String toString() {
        return "FieldInfo{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", sql='" + sql + '\'' +
                '}';
    }
}
