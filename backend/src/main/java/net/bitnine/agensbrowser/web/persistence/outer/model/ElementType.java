package net.bitnine.agensbrowser.web.persistence.outer.model;

import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.PropertyType;
import net.bitnine.agensbrowser.web.util.JsonbUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.*;

// **NOTE: Cytoscape.js 의 element 구조와 일치시키는게 편함
public class ElementType implements Serializable, Cloneable {

    private static final long serialVersionUID = -4488739207005029609L;

    protected String group;
    protected String id;
    protected String label;
    protected Map<String, Object> props;
    protected Map<String, Object> scratch;
    protected Object position;
    protected String classes;

    protected Long size;    // Schema graph 용, Accumulated graph 용
                            // 0L은 존재하지 않음을, 2L 이상은 여러개 존재함을 의미

    public ElementType() {
        this.group = "nodes";
        this.id = UUID.randomUUID().toString().replace("-","");
        this.label = "";
        this.props = new HashMap<String,Object>();
        this.scratch = new HashMap<String,Object>();
        this.classes = "";
        this.position = null;
        this.size = 1L;
    }
    public ElementType(String label) {
        this.group = "nodes";
        this.id = UUID.randomUUID().toString().replace("-","");
        this.label = label;
        this.props = new HashMap<String,Object>();
        this.scratch = new HashMap<String,Object>();
        this.classes = "";
        this.position = null;
        this.size = 1L;
    }
    // Schame GraphType 용 생성자
    public ElementType(LabelType label) {
        this.group = label.getType();
        this.id = label.getId();
        this.label = label.getType().toString();
        this.props = new HashMap<String,Object>();
        this.props.put("id", label.getId());
        this.props.put("name", label.getName());
        this.props.put("is_dirty", label.getIsDirty());
        this.props.put("owner", label.getOwner());
        this.props.put("desc", label.getDesc());
        this.props.put("size", label.getSize());
//        this.props.put("size_not_empty", label.getSizeNotEmpty());
        this.scratch = new HashMap<String,Object>();
        this.classes = "";
        this.position = null;
        this.size = label.getSize();
    }
    // convert Gremlin Element to ElementType
    public ElementType(Element ele){
        this.id = ele.id().toString();
        this.label = ele.label();
        this.size = 1L;
        this.props = new HashMap<String,Object>();
        this.scratch = new HashMap<String,Object>();
        this.classes = "";
        ele.properties().forEachRemaining(iter -> {
            // groupBy 에서 생성되는 $$size 도 있기 때문에 props 에 포함되도록 한다
            if( iter.key().equals("$$size") ) this.size = new Long(iter.value().toString());
            // **NOTE: 특수 변수 $$style, $$classes, $$position 등에 대한 처리
            //   ==> frontend에서 project load 시 graph_dto 읽으면서 확인
            if( iter.key().equals("$$style") ) this.scratch.put("_style", iter.value());
            else if( iter.key().equals("$$classes") ) this.classes = (String)iter.value();
            else if( iter.key().equals("$$position") ) this.position = iter.value();
            else if( iter.key().equals("$$state") ) this.scratch.put("_state", iter.value());
            else this.props.put( iter.key(), iter.value() );
        });
    }

    public String getGroup() { return group; }
    public void setGroup(String group) { this.group = group; }

    public String getPropertyId() {
        if( props.containsKey("id") ) return props.get("id").toString();
        return "";
    }
    public String getPropertyName() {
        String name = id;
        if( props.containsKey("name") ) name = props.get("name").toString();
        else if( props.containsKey("title") ) name = props.get("title").toString();
        return name;
    }

    public String getId() { return id; }
    public String getLabel() { return label; }
    public Map<String,Object> getProps() { return props; }
    public Map<String,Object> getScratch() { return scratch; }
    public Object getProperty(String key) { return props.get(key); }
    public Long getSize() { return size; }

    public void setId(String id) { this.id = id; }
    public void setLabel(String label) { this.label = label; }
    public void setProps(Map<String,Object> props) { this.props = props; }
    public void setScratch(Map<String,Object> scratch) { this.scratch = scratch; }
    public void setProperty(String key, Object value) { this.props.put(key, value); }
    public void setSize(Long size) { this.size = size; }

    @Override
    public String toString(){
        return "{\"" + this.getGroup() + "\": " + toJson().toJSONString() + "}";
    }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("id", id);
        // json.put("name", getPropertyName());         // don't need
        json.put("label", label);
        json.put("size", size);

        JSONObject jsonObject = new JSONObject();
        for(Map.Entry<String, Object> elem : props.entrySet()){
            jsonObject.put(elem.getKey(), elem.getValue());
        }
        json.put("props", jsonObject);

        JSONObject parent = new JSONObject();
        parent.put("group", this.getGroup());        // group = "elements"
        parent.put("data", json);

        JSONObject jsonTemp = new JSONObject();
        for(Map.Entry<String, Object> elem : scratch.entrySet()){
            jsonTemp.put(elem.getKey(), elem.getValue());
        }
        parent.put("scratch", jsonTemp);
        if( classes.length() > 0 ) parent.put("classes", classes);
        if( position != null ) parent.put("position", position);

        return parent;
    }

    @Override
    public int hashCode() {
        final int[] prime = { 17, 29, 31, 37, 73 };
        int result = prime[0]
                + prime[1] * ((this.group == null) ? 0 : this.group.hashCode())
                + prime[2] * ((this.id == null) ? 0 : this.id.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final ElementType other = (ElementType) obj;
        // type과 id가 같을 경우
        if (this.id != null && other.id != null && this.id.equals(other.id) )
            if (this.group != null && other.group != null && this.group.equals(other.group) ) return true;
        return false;
    }

    // Schema GraphType 용 LabelType 변환 : 1) LabelType 생성, 2) props 복사
    public LabelType getLabelType(){
        LabelType label = new LabelType(this.getLabel(), this.getGroup());
        for( Iterator<String> iter = props.keySet().iterator(); iter.hasNext(); ){
            String key = iter.next();
            String type = JsonbUtil.typeof(props.get(key));
            label.getProperties().add(new PropertyType( key, type ));
        }
        return label;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        ElementType ele = (ElementType) super.clone();
        // generic type 인 List labels 와 Map props 는 자동 처리
        return ele;
    }

}
