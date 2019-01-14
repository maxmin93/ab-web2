package net.bitnine.agensbrowser.web.message;

import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GraphDto extends ResponseDto implements Serializable {

    private static final long serialVersionUID = -1066428599528850641L;

    private Long gid;
    private GraphType graph;

    public GraphDto() {
        super();
        this.gid = -1L;
        this.graph = new GraphType();
    }
    public GraphDto(Long gid) {
        super();
        this.gid = gid;
        this.graph = new GraphType();
    }
    public GraphDto(Long gid, GraphType graph) {
        super();
        this.gid = gid;
        this.graph = graph;
    }

    public Long getGid() { return gid; }
    public GraphType getGraph() {
        return graph;
    }

    public void setGid(Long gid) { this.gid = gid; }
    public void setGraph(GraphType graph){ this.graph = graph; }

    @Override
    public String toString(){ return "{\"graph\": "+ toJson().toJSONString() + "}"; }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", "graph_dto");
        json.put("state", state.toString());
        json.put("message", message);

        json.put("gid", gid);
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
