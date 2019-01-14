package net.bitnine.agensbrowser.web.message;

import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ProjectDto implements Serializable {

    private static final long serialVersionUID = 3714942898323624874L;

    private Integer id;

    private String title;
    private String description;
    private String sql;
    private String image;         // mime-type: "image/png"
    private GraphType graph;

    public ProjectDto() {
        this.id = null;
        this.title = "";
        this.description = "";
        this.sql = "";
        this.graph = new GraphType();
        this.image = null;
    }
    public ProjectDto(Integer id, String title, String description, String sql, String image) {
        this.id = id;
        this.title = title;
        this.description = description;
        this.sql = sql;
        this.image = image;
        this.graph = new GraphType();
    }

    public Integer getId() { return id; }
    public String getTitle() { return title; }
    public String getDescription() { return description; }
    public String getSql() { return sql; }
    public GraphType getGraph() { return graph; }
    public String getImage() { return image; }

    public void setId(Integer id) { this.id = id; }
    public void setTitle(String title) { this.title = title; }
    public void setDescription(String description) { this.description = description; }
    public void setSql(String sql) { this.sql = sql; }
    public void setGraph(GraphType graph) { this.graph = graph; }
    public void setImage(String image) { this.image = image; }

    @Override
    public String toString(){ return "{\"project\": "+ toJson().toJSONString() + "}"; }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", "project");

        json.put("id", id);
        json.put("title", title);
        json.put("description", description);
        json.put("sql", sql);
        json.put("image", image);
        return json;
    }

    public List<Object> toJsonList(){
        List<Object> jsonList = new ArrayList<Object>();
        jsonList.add((Object) this.toJson());

        if( graph != null ) jsonList.addAll(graph.toJsonList());

        // group: end
        JSONObject json = new JSONObject();
        json.put("group", "end");
        jsonList.add(json);
        return jsonList;
    }

}
