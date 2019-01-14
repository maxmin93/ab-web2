package net.bitnine.agensbrowser.web.persistence.outer.model.type;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class PropertyType implements Cloneable {

    private String key;
    private Long size;

    public static enum ValueType { NUMBER, STRING, ARRAY, OBJECT, BOOLEAN, NULL, UNKNOWN };
    private ValueType type;

    public PropertyType(String key) {
        this.key = key;
        this.type = ValueType.UNKNOWN;
        this.size = 0L;
    }
    public PropertyType(String key, String type){
        this.key = key;
        this.type = toValueType(type);
        this.size = 0L;
    }
    public PropertyType(String key, String type, Long size){
        this.key = key;
        this.type = toValueType(type);
        this.size = size;
    }

    public static final ValueType toValueType(String valType){
        if( valType.equalsIgnoreCase("null") ) return ValueType.NULL;
        else if( valType.equalsIgnoreCase(ValueType.NUMBER.toString())
                || valType.equalsIgnoreCase(Integer.class.getName())
                || valType.equalsIgnoreCase(Long.class.getName())
                || valType.equalsIgnoreCase(Float.class.getName())
                || valType.equalsIgnoreCase(Double.class.getName())
        ) return ValueType.NUMBER;
        else if( valType.equalsIgnoreCase(ValueType.STRING.toString())
                || valType.equalsIgnoreCase(String.class.getName())
        ) return ValueType.STRING;
        else if( valType.equalsIgnoreCase(ValueType.ARRAY.toString())
                || valType.equalsIgnoreCase(JSONArray.class.getName())
        ) return ValueType.ARRAY;
        else if( valType.equalsIgnoreCase(ValueType.OBJECT.toString())
                || valType.equalsIgnoreCase(JSONObject.class.getName())
        ) return ValueType.OBJECT;
        else if( valType.equalsIgnoreCase(ValueType.BOOLEAN.toString())
                || valType.equalsIgnoreCase(Boolean.class.getName())
        ) return ValueType.BOOLEAN;
        else return ValueType.UNKNOWN;
    }

    public String getKey() { return key; }
    public Long getSize() { return size; }
    public String getType() { return type.toString(); }

    public void setKey(String key) { this.key = key; }
    public void setSize(Long size) { this.size = size; }
    public void setType(String type) { this.type = toValueType(type); }

    public void incrementSize() { this.size += 1; }

    @Override
    public String toString(){ return "{\"property\": "+ toJson().toJSONString() + "}"; }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("key",key);
        json.put("type",type.toString());
        json.put("size",size);
        return json;
    }

    @Override
    public int hashCode() {
        final int[] prime = { 17, 29, 31, 37, 73 };
        int result = prime[0];
        result = prime[1] * result + prime[2] * ((type == null) ? 0 : type.hashCode())
                + prime[3] * ((key == null) ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final PropertyType other = (PropertyType) obj;
        // key와 type이 같을 경우
        if (this.key != null && other.key != null && this.key.equals(other.key) )
            if (this.type != null && other.type != null && this.type.equals(other.type) )
                return true;
        return false;
    }

    public Object clone() throws CloneNotSupportedException {
        return (PropertyType) super.clone();
    }

}
