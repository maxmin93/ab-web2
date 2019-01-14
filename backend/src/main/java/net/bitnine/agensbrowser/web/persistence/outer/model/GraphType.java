package net.bitnine.agensbrowser.web.persistence.outer.model;

import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

public class GraphType implements Serializable, Cloneable {

    private static final long serialVersionUID = 603092706295629454L;

    // 추가 정보
    // : 군집계수, 군집수, 최단경로 집합 등
    // private GraphStatic static;

    private Set<LabelType> labels;
    private Set<NodeType> nodes;
    private Set<EdgeType> edges;

    public GraphType(){
        this.labels = new HashSet<LabelType>();
        this.nodes = new HashSet<NodeType>();
        this.edges = new HashSet<EdgeType>();
    }
    public GraphType(Set<NodeType> nodes, Set<EdgeType> edges) {
        this.labels = new HashSet<LabelType>();
        this.nodes = nodes;
        this.edges = edges;
    }
    public GraphType(Set<LabelType> labels, Set<NodeType> nodes, Set<EdgeType> edges) {
        this.labels = labels;
        this.nodes = nodes;
        this.edges = edges;
    }

    public Set<LabelType> getLabels() { return labels; }
    public Set<NodeType> getNodes() { return nodes; }
    public Set<EdgeType> getEdges() { return edges; }

    public void setLabels(Set<LabelType> labels) { this.labels = labels; }
    public void setNodes(Set<NodeType> nodes) { this.nodes = nodes; }
    public void setEdges(Set<EdgeType> edges) { this.edges = edges; }

    @Override
    public String toString() {
        return "{\"graph\": "+ toJson().toString() + "}";
    }

    public List<Object> toJsonList(){
        List<Object> jsonList = new ArrayList<Object>();

        // group: graph
        JSONObject json = new JSONObject();
        json.put("group", "graph");
        json.put("labels_size", labels.size());
        json.put("nodes_size", nodes.size());
        json.put("edges_size", edges.size());
        jsonList.add((Object) json);

        // group: labels
        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();){
            jsonList.add(iter.next().toJson());
        }
        // group: nodes
        for(Iterator<NodeType> iter = nodes.iterator(); iter.hasNext();){
            jsonList.add(iter.next().toJson());
        }
        // group: edges
        for(Iterator<EdgeType> iter = edges.iterator(); iter.hasNext();){
            jsonList.add(iter.next().toJson());
        }

        return jsonList;
    }

    public JSONObject toJson(){
        // group: graph
        JSONObject json = new JSONObject();
        json.put("group", "graph");
        json.put("labels_size", labels.size());
        json.put("nodes_size", nodes.size());
        json.put("edges_size", edges.size());

        // group: labels
        JSONArray jsonArray = new JSONArray();
        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();){
            jsonArray.add(iter.next().toJson());
        }
        json.put("labels", jsonArray);
        // group: nodes
        jsonArray = new JSONArray();
        for(Iterator<NodeType> iter = nodes.iterator(); iter.hasNext();){
            jsonArray.add(iter.next().toJson());
        }
        json.put("nodes", jsonArray);
        // group: edges
        jsonArray = new JSONArray();
        for(Iterator<EdgeType> iter = edges.iterator(); iter.hasNext();){
            jsonArray.add(iter.next().toJson());
        }
        json.put("edges", jsonArray);

        return json;
    }


    ////////////////////////////////////////////////////////////////////////

    // ** 참고
    // https://github.com/bitnine-oss/agensgraph-jdbc/blob/master/src/main/java/net/bitnine/agensgraph/graph/Path.java

    public void setValue(String value) throws SQLException {
        ArrayList<String> tokens = tokenize(value);
        for (int i = 0; i < tokens.size(); i++) {
            // **NOTE: NodeType-EdgeType-NodeType-EdgeType- 순으로 연결되는 구조라 치고!!
            // 짝수번에는 Node를 파싱
            if (i % 2 == 0) {
                NodeType node = new NodeType();
                node.setValue(tokens.get(i));
                nodes.add(node);
            }
            // 홀수번에는 Edge를 파싱
            else {
                EdgeType edge = new EdgeType();
                edge.setValue(tokens.get(i));
                edges.add(edge);
            }
        }
    }

    private ArrayList<String> tokenize(String value) throws SQLException {
        ArrayList<String> tokens = new ArrayList<>();

        // ignore wrapping '[' and ']' characters
        int pos = 1;
        int len = value.length() -1;

        int start = pos;
        int depth = 0;
        boolean gid = false;

        while (pos < len) {
            char c = value.charAt(pos);

            switch (c) {
                case '"':
                    if (depth > 0) {
                        // Parse "string".
                        // Leave pos unchanged if unmatched right " were found.
                        boolean escape = false;
                        for (int i = pos + 1; i < len; i++) {
                            c = value.charAt(i);
                            if (c == '\\') {
                                escape = !escape;
                            } else if (c == '"') {
                                if (escape)
                                    escape = false;
                                else {
                                    pos = i;
                                    break;
                                }
                            } else {
                                escape = false;
                            }
                        }
                    }
                    break;
                case '[':
                    if (depth == 0)
                        gid = true;
                    break;
                case ']':
                    if (depth == 0)
                        gid = false;
                    break;
                case '{':
                    depth++;
                    break;
                case '}':
                    depth--;
                    if (depth < 0) {
                        throw new PSQLException("Parsing GRAPH failed", PSQLState.DATA_ERROR);
                    }
                    break;
                case ',':
                    if (depth == 0 && !gid) {
                        tokens.add(value.substring(start, pos));
                        start = pos + 1;
                    }
                    break;
                default:
                    break;
            }

            pos++;
        }

        // ** add the last token
        tokens.add(value.substring(start, pos));

        return tokens;
    }

    public Object clone() throws CloneNotSupportedException {
        GraphType graphClone = (GraphType) super.clone();
        // Properties 들에 대해서도 각각 clone 시켜야 함

        Set<LabelType> metaClone = new HashSet<LabelType>();
        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext(); ){
            LabelType clone = (LabelType) iter.next().clone();
            metaClone.add(clone);
        }
        graphClone.setLabels(metaClone);

        Set<NodeType> nodesClone = new HashSet<NodeType>();
        for(Iterator<NodeType> iter = nodes.iterator(); iter.hasNext(); ){
            NodeType clone = (NodeType) iter.next().clone();
            nodesClone.add(clone);
        }
        graphClone.setNodes(nodesClone);

        Set<EdgeType> edgesClone = new HashSet<EdgeType>();
        for(Iterator<EdgeType> iter = edges.iterator(); iter.hasNext(); ){
            EdgeType clone = (EdgeType) iter.next().clone();
            edgesClone.add(clone);
        }
        graphClone.setEdges(edgesClone);

        return graphClone;
    }

}
/*
    ** 다중 간선(VLE: Variable Length EdgeType) 표현에 유의할 것! (여러개의 노드와 여러개의 에지)
ex) NodeType-EdgeType-NodeType
"[
    person[3.1]{\"id\": 718, \"name\": \"A., David\", \"gender\": \"m\", \"md5sum\": \"cf45e7b42fbc800c61462988ad1156d2\", \"name_pcode_cf\": \"A313\", \"name_pcode_nf\": \"D13\", \"surname_pcode\": \"A\"}
    ,actor_in[30.2744][3.1,4.3682715]{\"md5sum\": \"072e281b4317ad0a8b0bdb459d2a2ed4\", \"role_name\": \"Bartender\", \"name_pcode_nf\": \"B6353\"}
    ,production[4.3682715]{\"id\": 3673628, \"kind\": \"video movie\", \"title\": \"Once Upon a Secretary\", \"md5sum\": \"3aba7079347b80e2fd1f7b1430174f9b\", \"full_info\": [{\"plot\": \"Nina is a porn star. Sharon is a secretary who wants more than just sex from her married boss Mr. Murphy. Meanwhile, tired Nina falls asleep before sex with her boyfriend Brad. He therefore calls in their neighbor Elaine as substitute. Nina wakes up but just joins in until falling asleep again. Pressured for a less tiresome job, Nina applies for a strip bar, but gets too comfortable with the clients. Meanwhile, Mr. Murphy rewards Sharon after asking her to pleasure two businessmen. Realizing she's getting paid for sex, Sharon accepts her coworker Steve's offer to become a porn star. Steve introduces Sharon to Nina who shows Sharon the ropes. They decide trading jobs. Brad comes working for Mr. Murphy too, to protect Nina from the boss who has to overhear the couple's own office sex. Sharon convinces Steve to get on camera with her.\"}, {\"certificates\": \"USA:X\"}, {\"color info\": \"Color\"}, {\"countries\": \"USA\"}, {\"genres\": \"Adult\"}, {\"languages\": \"English\"}, {\"locations\": \"New York City, New York, USA\"}, {\"runtimes\": \"77\"}, {\"tech info\": \"OFM:Video\"}, {\"tech info\": \"RAT:1.33 : 1\"}], \"phonetic_code\": \"O5215\", \"production_year\": 1983}
]"

 */