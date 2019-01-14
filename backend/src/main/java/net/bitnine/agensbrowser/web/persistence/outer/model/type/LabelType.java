package net.bitnine.agensbrowser.web.persistence.outer.model.type;

import net.bitnine.agensbrowser.web.util.JsonbUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;

public class LabelType implements Cloneable {

    private String id;
    private String name;
    private String owner;
    private String desc;
    private String volume = "";

    private Long size;      // size of instances, not props

    private Boolean isDirty;        // whether data is changed or not for properties
    private List<PropertyType> properties;

    // 현재 데이터 상으로 연결되어 있는 neighbor nodes' name (edge도 node label명만 저장)
    private Set<String> source_neighbors;
    private Set<String> target_neighbors;

    private String type;

    public LabelType(){
        this.id = UUID.randomUUID().toString().replace("-","");
        this.name = "";
        this.type = "";
        this.owner = "";
        this.desc = "";
        this.size = 0L;
        this.isDirty = true;
        this.properties = new ArrayList<PropertyType>();
        this.source_neighbors = new HashSet<String>();
        this.target_neighbors = new HashSet<String>();
    }
    public LabelType(LabelType label){
        this.id = label.getId();
        this.name = label.getName();
        this.type = label.getType();
        this.owner = label.owner;
        this.desc = label.getDesc();
        this.size = 0L;
        this.isDirty = label.getIsDirty();
        this.properties = new ArrayList<PropertyType>();
        for( PropertyType prop : label.getProperties() ){
            this.properties.add( new PropertyType(prop.getKey(), prop.getType()) );
        }
        this.source_neighbors = new HashSet<String>();
        this.target_neighbors = new HashSet<String>();
    }
    public LabelType(String name, String type){
        this.id = UUID.randomUUID().toString().replace("-","");
        this.name = name;
        this.type = type;
        this.owner = "";
        this.desc = "";
        this.size = 0L;
        this.isDirty = true;
        this.properties = new ArrayList<PropertyType>();
        this.source_neighbors = new HashSet<String>();
        this.target_neighbors = new HashSet<String>();
    }
    public LabelType(String oid, String name, String type, String owner, String desc, String volume, long size){
        this.id = oid;
        this.name = name;
        this.type = type;
        this.owner = owner;
        this.desc = desc;
        this.size = new Long(size);
        this.isDirty = false;
        this.properties = new ArrayList<PropertyType>();
        this.source_neighbors = new HashSet<String>();
        this.target_neighbors = new HashSet<String>();
        this.volume = volume;
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public String getType() { return type; }
    public String getOwner() { return owner; }
    public String getDesc() { return desc; }
    public Long getSize() { return size; }
    public Boolean getIsDirty() { return isDirty; }
    public List<PropertyType> getProperties() { return properties; }
    public Set<String> getNeighbors() {
        Set<String> neighbors = new HashSet<String>();
        neighbors.addAll(source_neighbors);
        neighbors.addAll(target_neighbors);
        return neighbors;
    }
    public Set<String> getSourceNeighbors() { return source_neighbors; }
    public Set<String> getTargetNeighbors() { return target_neighbors; }
    public String getVolume(){ return volume; }

    public void setId(String id) { this.id = id; }
    public void setName(String name) { this.name = name; }
    public void setType(String type) { this.type = type; }
    public void setOwner(String owner) { this.owner = owner; }
    public void setDesc(String desc) { this.desc = desc; }
    public void setSize(Long size) { this.size = size; }
    public void setIsDirty(Boolean isDirty) { this.isDirty = isDirty; }
    public void setProperties(List<PropertyType> properties) { this.properties = properties; }
    public void setSourceNeighbors(Set<String> neighbors) { this.source_neighbors = neighbors; }
    public void setTargetNeighbors(Set<String> neighbors) { this.target_neighbors = neighbors; }
    public void setVolume(String volume) { this.volume = volume; }

    public void incrementSize() { this.size += 1; }
    public void resetSizeProperties(int val){
        for( Iterator<PropertyType> iter = properties.iterator(); iter.hasNext(); ){
            iter.next().setSize(Long.valueOf(val));
        }
    }
    public void incrementSizeProperties(Map<String,Object> props){
        for( String key : props.keySet() ){
            PropertyType property = null;
            for( PropertyType temp : properties ){
                if( temp.getKey().equals(key) ){
                    property = temp;
                    break;
                }
            }
            // 없으면 추가
            if( property == null ){
                property = new PropertyType(key, JsonbUtil.typeof( props.get(key) ));
                properties.add(property);
            }
            else{
                // 혹시 null or unknown type 이면 다시한번 type 확인
                if( props.get(key) != null ){
                    if( property.getType().equals(PropertyType.ValueType.NULL.toString())
                            || property.getType().equals(PropertyType.ValueType.UNKNOWN.toString()) )
                        property.setType(JsonbUtil.typeof( props.get(key) ));
                }
            }
            property.incrementSize();
        }
    }

    @Override
    public String toString() {
        return "{\"labels\": " + toJson().toJSONString() + "}";
    }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", "labels");

        json.put("id", id);
        json.put("name", name);
        json.put("type", type.toString());
        json.put("owner", owner);
        json.put("desc", desc);
        json.put("size", size);
        json.put("volume", volume);

        JSONArray propertyArray = new JSONArray();
        for(Iterator<PropertyType> iter = properties.iterator(); iter.hasNext();){
            propertyArray.add(iter.next().toJson());
        }
        json.put("properties", propertyArray);

        JSONArray sourcesArray = new JSONArray();
        for(Iterator<String> iter = source_neighbors.iterator(); iter.hasNext();){
            sourcesArray.add(iter.next());
        }
        json.put("sources", sourcesArray);
        JSONArray targetsArray = new JSONArray();
        for(Iterator<String> iter = target_neighbors.iterator(); iter.hasNext();){
            targetsArray.add(iter.next());
        }
        json.put("targets", targetsArray);

        JSONObject scratchObject = new JSONObject();
        scratchObject.put("is_dirty", isDirty);
        // default: _style = undefined;
        json.put("scratch", scratchObject);

        return json;
    }

    @Override
    public int hashCode() {
        final int[] prime = { 17, 29, 31, 37, 73 };
        int result = prime[0];
        result = prime[1] * result + prime[2] * ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final LabelType other = (LabelType) obj;
        // oid가 같을 경우
        if (this.id != null && other.id != null && this.id.equals(other.id) ) return true;
        // name과 type이 같을 경우 (id가 없는 어쩔수 없는 경우를 감안해)
        if (this.name != null && other.name != null && this.name.equals(other.name) )
            if (this.type != null && other.type != null && this.type.equals(other.type) )
                return true;
        return false;
    }

    public Object clone() throws CloneNotSupportedException {
        LabelType label = (LabelType) super.clone();
        // Properties 에 대해서도 각각 clone 시켜야 함
        List<PropertyType> cloneProperties = new ArrayList<PropertyType>();
        for( Iterator<PropertyType> iter = label.properties.iterator(); iter.hasNext(); ){
            PropertyType cloneProp = (PropertyType) iter.next().clone();
            cloneProperties.add(cloneProp);
        }
        label.setProperties(cloneProperties);
        // Neighbors 는 일반 타입이라 안해도 됨 (clone에서 처리됨)
        return label;
    }

}
