package net.bitnine.agensbrowser.web.message;

import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.DatasourceType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SchemaDto extends ResponseDto implements Serializable {

    private static final long serialVersionUID = -6913045165388361075L;

    private Boolean isDirty;
    private DatasourceType datasource;
    private List<LabelType> labels;
    private GraphType schemaGraph;

    public SchemaDto() {
        super();
        this.isDirty = true;
        this.datasource = new DatasourceType();
        this.labels = new ArrayList<LabelType>();
        this.schemaGraph = new GraphType();
    }
    public SchemaDto(Boolean isDirty, DatasourceType graph, List<LabelType> labels, GraphType meta) {
        super();
        this.isDirty = isDirty;
        this.datasource = graph;
        this.labels = labels;
        this.schemaGraph = meta;
    }

    public Boolean getDirty() { return isDirty; }
    public DatasourceType getDatasource() {
        return datasource;
    }
    public List<LabelType> getLabels() {
        return labels;
    }
    public GraphType getSchemaGraph() { return schemaGraph; }

    public void setDirty(Boolean dirty) { isDirty = dirty; }
    public void setDatasource(DatasourceType datasource){
        this.datasource = datasource;
    }
    public void setLabels(List<LabelType> labels){ this.labels = labels; }
    public void setSchemaGraph(GraphType schemaGraph) { this.schemaGraph = schemaGraph; }

    @Override
    public String toString(){ return "{\"schema\": "+ toJson().toJSONString() + "}"; }

    public JSONObject toJson(){
        // group: schema
        JSONObject json = new JSONObject();
        json.put("group", "schema");
        json.put("state", state.toString());
        json.put("message", message);

        json.put("is_dirty", isDirty);
        json.put("datasource", (datasource == null ? null : datasource.toJson()));
        JSONArray labelsArray = new JSONArray();
        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();){
            labelsArray.add(iter.next().toJson());
        }
        json.put("labels", labelsArray);
        return json;
    }

    public List<Object> toJsonList(){
        List<Object> jsonList = new ArrayList<Object>();
        jsonList.add((Object) this.toJson());

        jsonList.addAll(schemaGraph.toJsonList());

        // group: end
        JSONObject json = new JSONObject();
        json.put("group", "end");
        jsonList.add(json);
        return jsonList;
    }

}
