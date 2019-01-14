package net.bitnine.agensbrowser.web.persistence.outer.model.type;

import org.json.simple.JSONObject;

public class ColumnType {

    private String name;
    private Integer index;

    public static enum ValueType { NODE, EDGE, GRAPH, NUMBER, ID, STRING, ARRAY, OBJECT, BOOLEAN, NULL };
    private ValueType type;

    public ColumnType(String name) {
        this.name = name;
        this.type = ValueType.NULL;
        this.index = 0;
    }
    public ColumnType(String name, String type){
        this.name = name;
        this.type = toValueType(type);
        this.index = 0;
    }
    public ColumnType(String name, ValueType type, Integer index){
        this.name = name;
        this.type = type;
        this.index = index;
    }

    public static final ValueType toValueType(String valType){
        if( valType.equalsIgnoreCase(ValueType.NUMBER.toString()) ) return ValueType.NUMBER;
        else if( valType.equalsIgnoreCase(ValueType.STRING.toString()) ) return ValueType.STRING;
        else if( valType.equalsIgnoreCase(ValueType.ID.toString()) ) return ValueType.ID;
        else if( valType.equalsIgnoreCase(ValueType.ARRAY.toString()) ) return ValueType.ARRAY;
        else if( valType.equalsIgnoreCase(ValueType.OBJECT.toString()) ) return ValueType.OBJECT;
        else if( valType.equalsIgnoreCase(ValueType.NODE.toString()) ) return ValueType.NODE;
        else if( valType.equalsIgnoreCase(ValueType.EDGE.toString()) ) return ValueType.EDGE;
        else if( valType.equalsIgnoreCase(ValueType.GRAPH.toString()) ) return ValueType.GRAPH;
        else return ValueType.NULL;
    }

    public String getName() { return name; }
    public Integer getIndex() { return index; }
    public ValueType getType() { return type; }

    public void setName(String name) { this.name = name; }
    public void setIndex(Integer size) { this.index = index; }
    public void setType(String type) { this.type = toValueType(type); }
    public void setType(ValueType type) { this.type = type; }

    @Override
    public String toString(){ return "{\"columns\": "+ toJson().toJSONString() + "}"; }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", "columns");

        json.put("name", name);
        json.put("index", index);
        json.put("type", type.toString());
        return json;
    }
}
