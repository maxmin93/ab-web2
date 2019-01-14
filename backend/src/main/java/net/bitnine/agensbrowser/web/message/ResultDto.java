package net.bitnine.agensbrowser.web.message;

import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
import net.bitnine.agensbrowser.web.persistence.outer.model.RecordType;

import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ResultDto extends ResponseDto implements Serializable {

    private static final long serialVersionUID = 7998913526704164587L;

    private RequestDto request;

    private RecordType record;
    private GraphType graph;
    private Long gid = -1L;

    public ResultDto(){
        super();
        this.request = null;
        this.record = new RecordType();
        this.graph = new GraphType();
    }
    public ResultDto(RequestDto request){
        super();
        this.request = request;
        this.record = new RecordType();
        this.graph = new GraphType();
    }

    public RequestDto getRequest() { return request; }
    public RecordType getRecord() { return record; }
    public GraphType getGraph() { return graph; }
    public Long getGid() { return gid; }

    public void setRequest(RequestDto request) { this.request = request; }
    public void setRecord(RecordType record) { this.record = record; }
    public void setGraph(GraphType graph) { this.graph = graph; }
    public void setGid(Long gid) { this.gid = gid; }

    @Override
    public String toString(){ return "{\"result\": "+ toJson().toJSONString() + "}"; }

    public JSONObject toJson(){
        // group: result
        JSONObject json = new JSONObject();
        json.put("group", "result");
        json.put("state", state.toString());
        json.put("message", message);

        json.put("request", request.toJson());
        json.put("hasGraph", this.graph.getLabels().size() > 0 ? true : false );
        json.put("hasRecord", this.record.getCols().size() > 0 ? true : false );
        json.put("gid", gid);
        return json;
    }

    public List<Object> toJsonList(){
        List<Object> jsonList = new ArrayList<Object>();
        jsonList.add((Object) this.toJson());

        jsonList.addAll(graph.toJsonList());
        jsonList.addAll(record.toJsonList());

        // group: end
        JSONObject json = new JSONObject();
        json.put("group", "end");
        jsonList.add(json);
        return jsonList;
    }

}
