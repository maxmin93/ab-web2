package net.bitnine.agensbrowser.web.persistence.outer.model;

import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;

import net.bitnine.agensbrowser.web.util.JsonbUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.json.simple.JSONObject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EdgeType extends ElementType implements Serializable, Cloneable {

    private static final long serialVersionUID = -626958274497873643L;

    private static final Pattern edgePattern;
    static {
        edgePattern = Pattern.compile("(.+?)\\[(.+?)\\]\\[(.+?),(.+?)\\](.*)");
    }

    private String source;
    private String target;

    public EdgeType() {
        super();
        this.group = "edges";
        this.source = "";
        this.target = "";
    }
    public EdgeType(String label) {
        super(label);
        this.group = "edges";
        this.source = "";
        this.target = "";
    }
    // Schame GraphType 용 생성자
    public EdgeType(LabelType label, String source, String target) {
        super(label);
        this.group = "edges";
        this.source = source;
        this.target = target;
    }
    public EdgeType(Edge edge) {
        super((Element) edge);
        this.group = "edges";
        this.source = (String) edge.outVertex().id();
        this.target = (String) edge.inVertex().id();
    }

    public String getSource() { return source; }
    public String getTarget() { return target; }

    public void setSource(String source) { this.source = source; }
    public void setTarget(String target) { this.target = target; }

    @Override
    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("label", label);
        json.put("source", source);
        json.put("target", target);
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
        // if( position != null ) parent.put("position", position);     // edges don't have position

        return parent;

    }

    public void setValue(String value) throws SQLException {
        Matcher m = edgePattern.matcher(value);
        if (m.find()) {
            label = m.group(1).trim();
            id = m.group(2).trim();
            source = m.group(3).trim();
            target = m.group(4).trim();
            props = JsonbUtil.parseJsonToMap(m.group(5));
        } else {
            throw new PSQLException("Parsing EDGE failed", PSQLState.DATA_ERROR);
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        EdgeType edge = (EdgeType) super.clone();
        return edge;
    }

}
/*
"actor_in[30.2744][3.1,4.3682715]{\"md5sum\": \"072e281b4317ad0a8b0bdb459d2a2ed4\", \"role_name\": \"Bartender\", \"name_pcode_nf\": \"B6353\"}"

"actor_in[30.3711][3.3,4.3680861]{\"md5sum\": \"3d15b15825b2c1f1a7101fcc048a7d64\", \"role_name\": \"Mats\", \"name_pcode_nf\": \"M32\"}"

 */