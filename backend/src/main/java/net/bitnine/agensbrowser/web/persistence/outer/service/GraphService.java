package net.bitnine.agensbrowser.web.persistence.outer.service;

import com.google.common.collect.Iterators;
import net.bitnine.agensbrowser.web.message.*;
import net.bitnine.agensbrowser.web.persistence.inner.model.AgensProject;
import net.bitnine.agensbrowser.web.persistence.inner.service.AgensService;
import net.bitnine.agensbrowser.web.persistence.outer.model.EdgeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.ElementType;
import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
import net.bitnine.agensbrowser.web.persistence.outer.model.NodeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.PropertyType;
import net.bitnine.agensbrowser.web.service.FileStorageService;
import net.bitnine.agensbrowser.web.storage.GraphStorage;
import net.bitnine.agensbrowser.web.storage.SchemaStorage;

import net.bitnine.agensbrowser.web.util.JsonbUtil;
import net.bitnine.agensbrowser.web.util.TextPredicate;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.Frequency;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.base.Function;

import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphson;

import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

@Service
public class GraphService {

    private GraphStorage graphStorage;
    private SchemaStorage schemaStorage;
    private FileStorageService fileStorageService;
    private AgensService agensService;

    @Autowired
    public GraphService(
            FileStorageService fileStorageService,
            GraphStorage graphStorage,
            SchemaStorage schemaStorage,
            AgensService agensService
    ){
        this.fileStorageService = fileStorageService;
        this.graphStorage = graphStorage;
        this.schemaStorage = schemaStorage;
        this.agensService = agensService;
    }

    private TinkerGraph graphFactory(){
        BaseConfiguration conf = new BaseConfiguration();
        conf.setProperty("gremlin.tinkergraph.vertexIdManager","ANY");
        conf.setProperty("gremlin.tinkergraph.edgeIdManager","ANY");
        conf.setProperty("gremlin.tinkergraph.vertexPropertyIdManager","LONG");

        return TinkerGraph.open(conf);
    };

    public Long saveGraph(Long gid, GraphType source){
        // Graph 로 나타낼 Node 가 없으면 저장이고 뭐고 그냥 나가기
        if( source.getNodes().size() == 0 ) return -1L;

        // 저장할 대상 : nodes, edges
        Set<NodeType> nodes = source.getNodes();
        Set<EdgeType> edges = source.getEdges();

        TinkerGraph graph = null;
        if( graphStorage.hasGraph(gid) ) graph = graphStorage.getGraph(gid);
        else {
            graph = graphFactory();                 // 새로 생성후
            gid = graphStorage.addGraph(graph);     // 저장하고 신규 gid 반환
        }
        GraphTraversalSource g = graph.traversal();

        //
        // **NOTE : save 전략 (overwrite)
        //  1) 동일 id가 있는지 확인
        //  2) 없으면 추가, 있으면 덮어쓰기 (삭제후 추가?)
        //

        // group: nodes
        for(Iterator<NodeType> iter = nodes.iterator(); iter.hasNext();){
            NodeType node = iter.next();
            Vertex vertex = null;
            if( g.V().has(T.label,node.getLabel()).has(T.id, (Object)node.getId()).hasNext() ){
                vertex = g.V().has(T.label,node.getLabel()).has(T.id, (Object)node.getId()).next();
            }
            else{
                vertex = graph.addVertex(T.label, node.getLabel(), T.id, node.getId());
            }
            upsertProperties(vertex, node);
        }
        // group: edges
        for(Iterator<EdgeType> iter = edges.iterator(); iter.hasNext();){
            EdgeType edgeS = iter.next();
            Edge edgeT = null;
            if( g.E().has(T.label,edgeS.getLabel()).has(T.id, (Object)edgeS.getId()).hasNext() ){
                edgeT = g.E().has(T.label,edgeS.getLabel()).has(T.id, (Object)edgeS.getId()).next();
            }
            else {
                if (g.V(edgeS.getSource()).hasNext() && g.V(edgeS.getTarget()).hasNext()) {
                    Vertex sourceV = g.V(edgeS.getSource()).next();
                    Vertex targetV = g.V(edgeS.getTarget()).next();
                    edgeT = sourceV.addEdge(edgeS.getLabel(), targetV, T.id, edgeS.getId());
                }
            }
            upsertProperties(edgeT, edgeS);
        }

        try { g.close(); } catch (Exception e){
            System.out.println("[ERROR] saveGraph.close: "+e.getMessage());
        }

        return gid;
    }

    private int upsertProperties(Element target, ElementType source){
        int cnt = 0;
        for(Map.Entry<String,Object> elem : source.getProps().entrySet()){
            target.property(elem.getKey(), elem.getValue());
            cnt += 1;
        }
        return cnt;
    }

    private Vertex findVertexById(TinkerGraph graph, String id){
        GraphTraversalSource g = graph.traversal();
        Vertex vertex = null;
        if( g.V(id).hasNext() ) vertex = g.V(id).next();

        try {
            g.close();
        } catch (Exception e ){
            System.out.println("[ERROR] findVertexById: "+e.getMessage());
        }
        return vertex;
    }

    @Async("agensExecutor")
    public CompletableFuture<GraphType> getMetaGraph(Long gid) throws InterruptedException {
        TinkerGraph graph = graphStorage.getGraph(gid);
        if( graph == null ) return CompletableFuture.completedFuture( null );

        GraphTraversalSource g = graph.traversal();

        GraphType result = new GraphType();
        int countOfVertex = 0, countOfVertexWithData = 0;
        int countOfRelation = 0, countOfRelationWithData = 0;

        // Vertex에 대한 label별 카운팅
        Map<Object,Long> vLabelCnt = g.V().groupCount().by(T.label).next();
        for(Map.Entry<Object,Long> ele: vLabelCnt.entrySet()){
            LabelType nodeLabel = this.schemaStorage.findLabelTypeCopy("nodes", (String)ele.getKey());
            if( nodeLabel == null ) continue;

            // Node Label 찾아서 자신과 property 들의 count 저장
            NodeType node = new NodeType(nodeLabel);
            node.setSize( ele.getValue() );
            Map<Object,Long> labelPropsCnt = g.V().hasLabel(nodeLabel.getName()).properties().key().groupCount().next();
            node.setProperty("propsCount", labelPropsCnt);
            if( labelPropsCnt.size() > 0 ) countOfVertexWithData += 1;

            // schemaGraph의 nodes 리스트에 추가
            result.getNodes().add(node);
            countOfVertex += 1;
        }

        // Edge에 대한 label별 카운팅 : outV(source), inV(target) 포함 그룹핑
        Map<Object,Long> eLabelCnt = g.E().project("self","inL","outL")
                .by(__.label()).by(__.inV().label()).by(__.outV().label()).groupCount().next();
        for(Map.Entry<Object,Long> ele: eLabelCnt.entrySet()){
            Map<Object,Object> keyGroup = (Map<Object,Object>) ele.getKey();
            LabelType edgeLabel = null, sourceLabel = null, targetLabel = null;
            for(Map.Entry<Object,Object> item: keyGroup.entrySet()){
                String itemKey = (String) item.getKey();
                String itemVal = (String) item.getValue();
                if( itemKey.equals("self") )
                    edgeLabel = this.schemaStorage.findLabelTypeCopy("edges", itemVal);
                else if( itemKey.equals("inL") )
                    targetLabel = this.schemaStorage.findLabelTypeCopy("nodes", itemVal);
                else if( itemKey.equals("outL") )
                    sourceLabel = this.schemaStorage.findLabelTypeCopy("nodes", itemVal);
            }
            if( edgeLabel == null || sourceLabel == null || targetLabel == null ) continue;

            // Edge Label 찾아서 자신과 property 들의 count 저장
            EdgeType edge = new EdgeType(edgeLabel, sourceLabel.getId(), targetLabel.getId());
            edge.setSize( ele.getValue() );
            Map<Object,Long> labelPropsCnt = g.E().hasLabel(edgeLabel.getName()).properties().key().groupCount().next();
            edge.setProperty("propsCount", labelPropsCnt);
            if( labelPropsCnt.size() > 0 ) countOfRelationWithData += 1;

            // schemaGraph의 edges 리스트에 추가
            result.getEdges().add(edge);
            countOfRelation += 1;
        }

        // metaLabels에 NodeType, Edge를 의미하는 LabelType 을 임의로 만들어 넣기 (카운트 저장용)
        // : LabelType(String oid, String name, String type, String owner, String desc)
        LabelType metaNodeType = new LabelType("NODE", "nodes");
        metaNodeType.setSize(new Long(countOfVertex));                      // 전체 NodeType 카운트
//        metaNodeType.setSizeNotEmpty(new Long(countOfVertexWithData));      // 데이터가 있는 NodeType 카운트
        result.getLabels().add(metaNodeType);
        LabelType metaEdgeType = new LabelType("EDGE", "edges");
        metaEdgeType.setSize(new Long(countOfRelation));                    // 전체 EdgeType 카운트
//        metaEdgeType.setSizeNotEmpty(new Long(countOfRelationWithData));    // 데이터가 있는 EdgeType 카운트
        result.getLabels().add(metaEdgeType);

        try{ g.close(); }catch(Exception e){ System.out.println("getMetaGraph: close error, gid="+gid); }
        return CompletableFuture.completedFuture( result );
    }

    private GraphType convertGraphType(TinkerGraph graph){
        GraphType result = new GraphType();
        GraphTraversalSource g = graph.traversal();

        // make graph.nodes
        GraphTraversal nodeIter = g.V();
        while( nodeIter.hasNext() ){
            Vertex vertex = (Vertex) nodeIter.next();
            result.getNodes().add( new NodeType(vertex) );
        }

        // make graph.edges
        GraphTraversal edgeIter = g.E();
        while( edgeIter.hasNext() ){
            Edge edge = (Edge) edgeIter.next();
            result.getEdges().add( new EdgeType(edge) );
        }

        ////////////////////////////////////////////////////////
        // make graph.labels

        // Vertex에 대한 label별 카운팅
        Map<Object,Long> vLabelCnt = g.V().groupCount().by(T.label).next();
        for(Map.Entry<Object,Long> ele: vLabelCnt.entrySet()){
            LabelType temp = this.schemaStorage.findLabelTypeCopy("nodes", (String)ele.getKey());
            LabelType label = new LabelType((String)ele.getKey(), "nodes");
            if( temp != null ){
                try{
                    label = (LabelType) temp.clone();
                }catch( CloneNotSupportedException ce ){
                    System.out.println("upsertLabel('"+(String)ele.getKey()+"'): CloneNotSupportedException->"+ce.getMessage()+"\n");
                }
            }
            // label 기준 count
            label.setSize( ele.getValue() );
            // property 기준 count
            Map<Object,Long> labelPropsCnt = g.V().hasLabel(label.getName()).properties().key().groupCount().next();
            for( Map.Entry<Object,Long> pcnt : labelPropsCnt.entrySet() ){
                // ==> schemaStorage에 properties 가 없기 때문에 바로 생성해서 추가
                String pkey = (String)pcnt.getKey();
                // $$ variables 이면 속성생성 건너뛰기
                if( pkey.equals("$$style") || pkey.equals("$$classes") || pkey.equals("$$position") ) continue;
                PropertyType prop = new PropertyType(pkey);
                Object sample = g.V().hasLabel(label.getName()).values(pkey).sample(1).next();
                prop.setType( JsonbUtil.typeof(sample) );
                prop.setSize( pcnt.getValue() );
                label.getProperties().add( prop );
            }
            // source_neighbors
            label.getSourceNeighbors().addAll( g.V().hasLabel(label.getName()).in().label().dedup().toSet() );
            // target_neighbors
            label.getTargetNeighbors().addAll( g.V().hasLabel(label.getName()).out().label().dedup().toSet() );
            // append to graph.labels
            result.getLabels().add( label );
        }

        // Edge에 대한 label별 카운팅 : outV(source), inV(target) 포함 그룹핑
        Map<Object,Long> eLabelCnt = g.E().groupCount().by(T.label).next();
        for(Map.Entry<Object,Long> ele: eLabelCnt.entrySet()){
            LabelType temp = this.schemaStorage.findLabelTypeCopy("edges", (String)ele.getKey());
            LabelType label = new LabelType((String)ele.getKey(), "edges");
            if( temp != null ){
                try{
                    label = (LabelType) temp.clone();
                }catch( CloneNotSupportedException ce ){
                    System.out.println("upsertLabel('"+(String)ele.getKey()+"'): CloneNotSupportedException->"+ce.getMessage()+"\n");
                }
            }
            // label 기준 count
            label.setSize( ele.getValue() );
            // property 기준 count
            // ==> schemaStorage에 properties 가 없기 때문에 바로 생성해서 추가
            Map<Object,Long> labelPropsCnt = g.E().hasLabel(label.getName()).properties().key().groupCount().next();
            for( Map.Entry<Object,Long> pcnt : labelPropsCnt.entrySet() ){
                String pkey = (String)pcnt.getKey();
                // $$ variables 이면 속성생성 건너뛰기
                if( pkey.equals("$$style") || pkey.equals("$$classes") || pkey.equals("$$position") ) continue;
                PropertyType prop = new PropertyType(pkey);
                Object sample = g.E().hasLabel(label.getName()).values(pkey).sample(1).next();
                prop.setType( JsonbUtil.typeof(sample) );
                prop.setSize( pcnt.getValue() );
                label.getProperties().add( prop );
            }
            // source_neighbors
            label.getSourceNeighbors().addAll( g.E().hasLabel(label.getName()).outV().label().dedup().toSet() );
            // target_neighbors
            label.getTargetNeighbors().addAll( g.E().hasLabel(label.getName()).inV().label().dedup().toSet() );
            // append to graph.labels
            result.getLabels().add( label );
        }

        try{ g.close(); }catch(Exception e){ System.out.println("convertGraphType: close error"); }
        return result;
    }

    @Async("agensExecutor")
    public CompletableFuture<GraphDto> newGraph(ClientDto client) throws InterruptedException {
        TinkerGraph graph = graphFactory();
        if( graph == null ) return CompletableFuture.completedFuture( null );

        Long gid = graphStorage.addGraph(graph);    // storage 에 등록
        client.setGid(gid);                         // client 에 gid 설정

        GraphDto result = new GraphDto(gid, new GraphType());   // 빈 그래프
        result.setState(ResponseDto.StateType.SUCCESS);
        result.setMessage(String.format("New graph[%d] is ready (created by '%s')", gid, client.getUserName()));
        return CompletableFuture.completedFuture( result );
    }

    @Async("agensExecutor")
    public CompletableFuture<GraphType> getGraph(Long gid) throws InterruptedException {
        TinkerGraph graph = graphStorage.getGraph(gid);
        if( graph == null ) return CompletableFuture.completedFuture( null );

        return CompletableFuture.completedFuture( convertGraphType(graph) );
    }

    // copy properties: TinkerGraph -> TinkerGraph
    private void copyProperties(Element target, Element source){
        for (String key : source.keys()){
            target.property( key, source.value(key));
        }
    }
    // copy properties: CyElement -> TinkerGraph
    private void copyProperties(Element target, ElementType source){
        // Client 에서 보내온 Element의 property 들을 복사 또는 갱신
        for( Map.Entry<String, Object> prop : source.getProps().entrySet() ){
            target.property(prop.getKey(), prop.getValue());
        }
        // updateGraph 에서 _deleted 속성들은 삭제
        if( source.getScratch().containsKey("_deleted") ){
            List<String> deleted = (List<String>) source.getScratch().get("_deleted");
            for( String key : deleted ){
                if( target.property(key).isPresent() ) target.property(key).remove();
            }
        }
    }

    ////////////////////////////////////
    // **NOTE: self re-direction 인 경우에 대한 처리가 완전하지 않다
    //      ==> ex: (customer{country='USA'})-[]->(customer{country='UK'})
    //          이런 경우, tLabel 이 같다고 건너뛸 것이 아니라 newV 들이 모두 생성된 후 edge 연결을 해야 함
    //
    private void doGroupBy(TinkerGraph tGraph, TinkerGraph sGraph, String tLabel, List<String> propArr){
        String[] props = propArr.toArray(new String[propArr.size()]);
        List<Vertex> newVGrp = new ArrayList<>();

        GraphTraversalSource g = sGraph.traversal();
        GraphTraversalSource gt = tGraph.traversal();

        // props 속성 리스트에 대한 groupBy : HashMap<List, List>
        // ==>[Finland, Oulu]=[v[8.87]]
        HashMap<Object,Object> grpMap = (HashMap<Object,Object>) g.V().hasLabel(tLabel).group().by(__.values(props).fold()).next();
        for(HashMap.Entry<Object,Object> ele: grpMap.entrySet()){
            List<Object> pList = (List<Object>) ele.getKey();
            List<Vertex> vList = (List<Vertex>) ele.getValue();
            List<String> vidList = vList.stream().map(x -> (String) x.id()).collect(Collectors.toList());
            // props = ['$ALL'] 일 경우에는 pList 는 empty array 반환, vidList 에는 tLabel의 vid 전체가 담긴 array 반환

            // 새로운 group Vertex 생성 : id = "$$parent_"+randomUUID
            Vertex newV = tGraph.addVertex(T.label, tLabel, T.id, "$$parent_"+UUID.randomUUID().toString().replace("-",""));
            newV.property("name", pList.size() == 0 ? tLabel+" ($ALL)" : pList.toString() );
            newV.property("$$groupBy", props );   // String.join(",", props));
            newV.property("$$members", vidList ); // String.join(",", vidList));
            newV.property("$$size", vidList.size() );
            newVGrp.add(newV);

            // EdgeType 생성 후 source, target 재지정
            for(Vertex v : vList){
                List<Edge> outEList = g.V(v).outE().toList();
                for( Edge outE : outEList ){
                    // out Edge의 target Vertex
                    Vertex outV = null;
                    if( gt.V(outE.inVertex().id()).hasNext() ) outV = gt.V(outE.inVertex().id()).next();
                    else {
                        // 연결된 vertex 가 같은 tLabel 이 아니면 (ex: customer-[]->!customer)
                        if( !outE.inVertex().label().equals(tLabel) ) {
                            outV = tGraph.addVertex(T.label, outE.inVertex().label(), T.id, outE.inVertex().id());
                            copyProperties(outV, outE.inVertex());
                        }
                    }
                    // self re-direction 인 경우 outV == null
                    if( outV != null && !outV.label().equals(tLabel) ){
                        Edge newE = newV.addEdge(outE.label(), outV, T.id, outE.id());  // copy edge
                        copyProperties(newE, outE);
                    }
                }
                List<Edge> inEList = g.V(v).inE().toList();
                for( Edge inE: inEList ){
                    // in Edge 의 source Vertex
                    Vertex inV = null;
                    if( gt.V(inE.outVertex().id()).hasNext() ) inV = gt.V(inE.outVertex().id()).next();
                    else {
                        // 연결된 vertex 가 같은 tLabel 이 아니면 (ex: customer-[]->!customer)
                        if( !inE.outVertex().label().equals(tLabel) ) {
                            inV = tGraph.addVertex(T.label, inE.outVertex().label(), T.id, inE.outVertex().id());
                            copyProperties(inV, inE.outVertex());
                        }
                    }
                    // self re-direction 인 경우 inV == null
                    if( inV != null && !inV.label().equals(tLabel) ){
                        Edge newE = inV.addEdge(inE.label(), newV, T.id, inE.id());     // copy edge
                        copyProperties(newE, inE);
                    }
                }
            }
        }

        // self re-direction 인 경우의 edge 생성 : VG x VG 모든 조합에 대한 inE, outE 검사
        for(Vertex vL: newVGrp) {
            List<String> vListL = (List<String>) vL.property("$$members").value();
            String nameL = "{"+String.join(",", (String[]) vL.property("$$groupBy").value())+"}";
            for(Vertex vR: newVGrp) {
                List<String> vListR = (List<String>) vR.property("$$members").value();
                String nameR = "{"+String.join(",", (String[]) vR.property("$$groupBy").value())+"}";

                // case) vL %s -> vR %s
                if (g.V(vListL).outE().as("a").V(vListR).inE().as("b").where("a", P.eq("b")).hasNext()) {
                    Map<Object, Object> selfGrp = g.V(vListL).outE().as("a").V(vListR).inE().as("b").where("a", P.eq("b")).group().by(T.label).by(__.count()).next();
                    for (HashMap.Entry<Object, Object> eGrp : selfGrp.entrySet()) {
                        String eLabel = eGrp.getKey().toString();
                        int eSize = Integer.parseInt(eGrp.getValue().toString());
//                        System.out.println(String.format("-- groupBy Self-In: L %s x R %s ==> %s : %d", vListL.toString(), vListR.toString(), eLabel, eSize));
                        Edge newE = vL.addEdge(eLabel, vR, T.id, "$$parent_"+UUID.randomUUID().toString().replace("-",""));
                        newE.property("name", nameL+"->"+nameR);
                        newE.property("$$size", eSize);
                    }
                }
                // case) vL %s <- vR %s
                if (g.V(vListL).inE().as("a").V(vListR).outE().as("b").where("a", P.eq("b")).hasNext()) {
                    Map<Object, Object> selfGrp = g.V(vListL).inE().as("a").V(vListR).outE().as("b").where("a", P.eq("b")).group().by(T.label).by(__.count()).next();
                    for (HashMap.Entry<Object, Object> eGrp : selfGrp.entrySet()) {
                        String eLabel = eGrp.getKey().toString();
                        int eSize = Integer.parseInt(eGrp.getValue().toString());
//                        System.out.println(String.format("-- groupBy Self-Out: L %s x R %s ==> %s : %d", vListL.toString(), vListR.toString(), eLabel, eSize));
                        Edge newE = vR.addEdge(eLabel, vL, T.id, "$$parent_"+UUID.randomUUID().toString().replace("-",""));
                        newE.property("name", nameR+"->"+nameL);
                        newE.property("$$size", eSize);
                    }
                }
            }
        }

        // target label 과 both() vertexes 이 아닌 rest vertexes
        Set<Vertex> tnodes = g.V().hasLabel(tLabel).toSet();
        tnodes.addAll( g.V().hasLabel(tLabel).both().dedup().toSet() );
        List<Vertex> rnodes = g.V().toStream().filter(e -> !tnodes.contains(e)).collect(Collectors.toList());
        for( Vertex v : rnodes ){
            // copy rest vertexes
            Vertex newV = tGraph.addVertex(T.label, v.label(), T.id, v.id());
            copyProperties(newV, v);
        }

        // target label 에 연결된 edge 가 아닌 rest edges
        List<Edge> tedges = g.V().hasLabel(tLabel).bothE().toList();
        List<Edge> redges = g.E().toStream().filter(e -> !tedges.contains(e)).collect(Collectors.toList());
        for( Edge e: redges ){
            // copy rest edges
            if( gt.V( e.outVertex().id() ).hasNext() && gt.V( e.inVertex().id() ).hasNext() ){
                Vertex outV = gt.V( e.outVertex().id() ).next();
                Vertex inV = gt.V( e.inVertex().id() ).next();
                Edge newE = outV.addEdge(e.label(), inV, T.id, e.id());
                copyProperties(newE, e);
            }
        }
        System.out.println(String.format("groupBy: %s<%s> ==> %s", tLabel, String.join(",",props), tGraph.toString()));

        try{
            gt.close();
            g.close();
        } catch(Exception e) { System.out.println("groupBy: close error"); }
    }

    // 참고 http://tinkerpop.apache.org/docs/current/recipes/#duplicate-edge
    // source 와 target 이 동일한 1개 이상의 edges 리스트 출력
    // ==> [e[13][1-created->3],e[9][1-created->3]]
    //
    private List<List<Edge>> detectDuplicateEdges(TinkerGraph graph){
        GraphTraversalSource g = graph.traversal();

        GraphTraversal dupIter = g.V().match(
                __.as("ov").outE().as("e"),
                __.as("e").inV().as("iv"),
                __.as("iv").inE().as("ie"),
                __.as("ie").outV().as("ov"))
                .where("ie",P.neq("e"))
                .where("ie",P.eq("e")).by(T.label)
                .select("ie")
                .group()
                    .by(__.select("ov","e","iv").by().by(T.label))
                .unfold().select(Column.values)
                .where(__.count(Scope.local).is(P.gt(1)));

        List<List<Edge>> dupElist = new ArrayList<>();
        while( dupIter.hasNext() ) {
            Set<Edge> edges = new HashSet<Edge>((List<Edge>) dupIter.next());
            dupElist.add( new ArrayList<>(edges) );
        }

        ////////////////////////////////////////////
        // **NOTE: drop Edge 가 적용되지 않음!!
        // ==> sGraph 에서 tGraph 를 복사하는 방식으로 임시 변통함
        //
//        for( List<String> dupRow : dupElist ){
//            int i = 0;
//            String firstId = null;
//            for( String eid : dupRow ){
//                if( i > 0 ) {
//                    System.out.println("drop edge: "+eid);
//                    g.E( eid ).drop();
//                }
//                else firstId = eid;
//            }
//            if( firstId != null ){
//                System.out.println("  dupArr: "+dupRow.toString()+" to "+firstId);
//                g.E( firstId ).property("dupArr", dupRow);
//            }
//        }

        try{ g.close(); } catch(Exception e) { System.out.println("detectDuplicateEdges: close error"); }
        return dupElist;
    }

    private void dropDuplicateEdges(TinkerGraph tGraph, TinkerGraph sGraph, List<List<Edge>> dupElist){
        GraphTraversalSource g = sGraph.traversal();
        GraphTraversalSource gt = tGraph.traversal();

        // target label 과 both() vertexes 이 아닌 rest vertexes
        List<Vertex> vertices = g.V().toList();
        for( Vertex v : vertices ){
            // copy rest vertexes
            Vertex newV = tGraph.addVertex(T.label, v.label(), T.id, v.id());
            copyProperties(newV, v);
        }

        // duplicate edges 가 아닌 rest edges
        List<Edge> tedges = dupElist.stream().flatMap(List::stream).collect(Collectors.toList());
        List<Edge> redges = g.E().toStream().filter(e -> !tedges.contains(e)).collect(Collectors.toList());
        for( Edge e: redges ){
            // copy rest edges
            if( gt.V( e.outVertex().id() ).hasNext() && gt.V( e.inVertex().id() ).hasNext() ){
                Vertex outV = gt.V( e.outVertex().id() ).next();
                Vertex inV = gt.V( e.inVertex().id() ).next();
                Edge newE = outV.addEdge(e.label(), inV, T.id, e.id());
                copyProperties(newE, e);
            }
        }

        // 각 duplicate edges 에서 첫번째 edge만 추가
        for( List<Edge> dupRow : dupElist ){
            String[] dupArr = dupRow.stream().map(x -> x.id().toString()).toArray(String[]::new);
            Edge e = dupRow.get(0);
            int size = 0;       // $$size 가 있으면 합산해서 size 계산 (없으면 1)
            for( Edge tmp : dupRow ){
                if( tmp.property("$$size").isPresent() )
                    size += Integer.parseInt( tmp.property("$$size").value().toString() );
                else size += 1;
            }
            // compress duplicated edges by new edge
            if( gt.V( e.outVertex().id() ).hasNext() && gt.V( e.inVertex().id() ).hasNext() ){
                Vertex outV = gt.V( e.outVertex().id() ).next();
                Vertex inV = gt.V( e.inVertex().id() ).next();
                // 새로운 group Edge 생성 : id = "$$parent_"+randomUUID
                Edge newE = outV.addEdge(e.label(), inV, T.id, "$$parent_"+UUID.randomUUID().toString().replace("-",""));
//                newE.property("$$sourceV", outV.property("$$groupBy").value());
//                newE.property("$$targetV", inV.property("$$groupBy").value());
                newE.property("$$members", dupArr);
                newE.property("$$size", size );
            }
        }

        try{ g.close(); gt.close();
        } catch(Exception e) { System.out.println("dropDuplicateEdges: close error"); }
    }

    private void doFilterBy(TinkerGraph tGraph, TinkerGraph sGraph, String tlabel, List<List<Object>> propArr){
        GraphTraversalSource g = sGraph.traversal();
        GraphTraversalSource gt = tGraph.traversal();

        GraphTraversal ts = g.V().hasLabel(tlabel);
        GraphTraversal tt = null;

        System.out.print(String.format("filterBy: %s[%d] ==> ", tlabel, g.V().hasLabel(tlabel).count().next()));
        for( List<Object> pRow : propArr ){
            if( pRow.size() < 3 ) continue;
            Object compValue = pRow.size() >= 4 && pRow.get(3).equals("NUMBER") ? pRow.get(2).toString() : "\""+pRow.get(2).toString()+"\"";
            // System.out.print(String.format("<%s> %s %s,", pRow.get(0).toString(), pRow.get(1).toString(), compValue) );
            switch( (String) pRow.get(1) ){
                case "eq": tt = ts.has(pRow.get(0).toString(), P.eq( pRow.get(2) )); break;
                case "neq": tt = ts.has(pRow.get(0).toString(), P.neq( pRow.get(2) )); break;
                case "gt": tt = ts.has(pRow.get(0).toString(), P.gt( pRow.get(2) )); break;
                case "lt": tt = ts.has(pRow.get(0).toString(), P.lt( pRow.get(2) )); break;
                case "gte": tt = ts.has(pRow.get(0).toString(), P.gte( pRow.get(2) )); break;
                case "lte": tt = ts.has(pRow.get(0).toString(), P.lte( pRow.get(2) )); break;
                case "contains": tt = ts.has(pRow.get(0).toString(), TextPredicate.contains( pRow.get(2) )); break;
                case "notContains": tt = ts.has(pRow.get(0).toString(), TextPredicate.notContains( pRow.get(2) )); break;
                case "startsWith": tt = ts.has(pRow.get(0).toString(), TextPredicate.startsWith( pRow.get(2) )); break;
                case "endsWith": tt = ts.has(pRow.get(0).toString(), TextPredicate.endsWith( pRow.get(2) )); break;
                default: tt = ts;
            }
            ts = tt;    // 마지막엔 돌고 돌아 ts 로 연결
        }
        List<Vertex> vList = ts.toList();
        // System.out.print(String.format(" ==> %s[%d]\n", tlabel, vList.size()));
        for( Vertex v : vList ){
            // copy rest vertexes
            Vertex newV = tGraph.addVertex(T.label, v.label(), T.id, v.id());
            copyProperties(newV, v);
        }

        // target label 이 아닌 rest vertexes
        List<Vertex> tList = g.V().hasLabel(tlabel).toList();
        List<Vertex> rList = g.V().toStream().filter(e -> !tList.contains(e)).collect(Collectors.toList());
        for( Vertex v : rList ){
            // copy rest vertexes
            Vertex newV = tGraph.addVertex(T.label, v.label(), T.id, v.id());
            copyProperties(newV, v);
        }

        // rest edges
        List<Edge> eList = g.E().toList();
        for( Edge e: eList ){
            // outV, inV 가 모두 존재하는 edge 만 복사
            if( gt.V( e.outVertex().id() ).hasNext() && gt.V( e.inVertex().id() ).hasNext() ){
                Vertex outV = gt.V( e.outVertex().id() ).next();
                Vertex inV = gt.V( e.inVertex().id() ).next();
                Edge newE = outV.addEdge(e.label(), inV, T.id, e.id());
                copyProperties(newE, e);
            }
        }
    }

    private GraphTraversal updateFilterPredicate(GraphTraversal t, List<Object> pRow){
        GraphTraversal ut = null;
        String prop = (String) pRow.get(0);
        String oper = (String) pRow.get(1);
        Object value = pRow.get(2);

        return ut;
    }

    private String mergeGraph(TinkerGraph tGraph, TinkerGraph sGraph) {
        GraphTraversalSource g = sGraph.traversal();
        GraphTraversalSource gt = tGraph.traversal();
        int vcnt = 0, ecnt = 0;

        // copy vertexes
        List<Vertex> vertices = g.V().toList();
        for (Vertex v : vertices) {
            if( !gt.V(v.id()).hasNext() ) {
                Vertex newV = tGraph.addVertex(T.label, v.label(), T.id, v.id());
                copyProperties(newV, v);
                vcnt += 1;
            }
        }
        // copy edges
        List<Edge> edges = g.E().toList();
        for (Edge e : edges) {
            if (gt.V(e.outVertex().id()).hasNext() && gt.V(e.inVertex().id()).hasNext()) {
                if( !gt.E(e.id()).hasNext() ) {
                    Vertex outV = gt.V(e.outVertex().id()).next();
                    Vertex inV = gt.V(e.inVertex().id()).next();
                    Edge newE = outV.addEdge(e.label(), inV, T.id, e.id());
                    copyProperties(newE, e);
                    ecnt += 1;
                }
            }
        }
        try{ g.close(); gt.close();
        }catch(Exception e){ System.out.println("mergeGraph: close error"); }

        return String.format("mergeGraph: vcnt=%d, ecnt=%d", vcnt, ecnt);
    }

/*
{
	"filters" : {
		"customer": [ ["country", "notContains", "nce"], ["p_length", "gte", 20] ]
	},
	"groups": {
		"customer": ["country"],
		"order": ["shipcountry"]
	}
}
 */
    @Async("agensExecutor")
    public CompletableFuture<GraphType> filterNgroupBy(Long gid, FiltersGroupsDto params) throws InterruptedException {
        TinkerGraph sGraph = graphStorage.getGraph(gid);
        if( sGraph == null ) return CompletableFuture.completedFuture( null );

        TinkerGraph tGraph = graphFactory();    // empty graph
        mergeGraph(tGraph, sGraph);             // copy for protecting original graph

        sGraph = tGraph;
        tGraph = graphFactory();                // empty graph

        int i = 0;
        for( Map.Entry<String,List<List<Object>>> param : params.getFilters().entrySet() ) {
            doFilterBy(tGraph, sGraph, param.getKey(), param.getValue());       // groupBy : sGraph --> tGraph
            try{
                if( i > 0 ) sGraph.close();
                i += 1;
            }catch(Exception e){ System.out.println("filterNgroupBy: close error in filters loop"); }

            sGraph = tGraph;
            tGraph = graphFactory();            // empty graph
        }

        for( Map.Entry<String,List<String>> param : params.getGroups().entrySet() ) {
            doGroupBy(tGraph, sGraph, param.getKey(), param.getValue());       // groupBy : sGraph --> tGraph
            try{
                if( i > 0 ) sGraph.close();
                i += 1;
            }catch(Exception e){ System.out.println("filterNgroupBy: close error in groups loop"); }

            sGraph = tGraph;
            tGraph = graphFactory();            // empty graph
        }
        List<List<Edge>> dupElist = detectDuplicateEdges(sGraph);       // tGraph, sGraph
        dropDuplicateEdges(tGraph, sGraph, dupElist);
        GraphType result = convertGraphType(tGraph);

        try{ sGraph.close(); tGraph.close();
        }catch(Exception e){ System.out.println("filterNgroupBy: close error"); }
        return CompletableFuture.completedFuture( result );
    }

    @Async("agensExecutor")
    public CompletableFuture<GraphType> getGroupByGraph(Long gid, String tlabel, String[] props) throws InterruptedException {
        TinkerGraph sGraph = graphStorage.getGraph(gid);
        if( sGraph == null ) return CompletableFuture.completedFuture( null );

        TinkerGraph tGraph = graphFactory();            // empty graph
        doGroupBy(tGraph, sGraph, tlabel, Arrays.asList(props));       // groupBy : sGraph --> tGraph
        GraphType result = convertGraphType(tGraph);

        try{ tGraph.close(); }catch(Exception e){ System.out.println("getGroupByGraph: close error"); }
        return CompletableFuture.completedFuture( result );
    }

    // convert List<List<Long>> to List<List<Object>> and return
    @Async("agensExecutor")
    public CompletableFuture<List<List<Object>>> getList() throws InterruptedException {
        List<List<Long>> listLong = graphStorage.getList();
        List<List<Object>> listObject = new ArrayList<List<Object>>();
        for(Iterator<List<Long>> iter = listLong.iterator(); iter.hasNext();){
            listObject.add( new ArrayList<Object>( iter.next() ) );
        }
        return CompletableFuture.completedFuture( listObject );
    }

    /////////////////////////////////////////////////////////////////////////

    @Async("agensExecutor")
    public CompletableFuture<ResponseDto> matchProjectTest(Integer pid, List<String> ids){

        AgensProject project = agensService.findOneProjectById(pid);
        if( project == null || project.getGryo_data() == null )
            return CompletableFuture.completedFuture( null );

        TinkerGraph graph = graphFactory();          // 새로 생성후
        final InputStream is = new ByteArrayInputStream(project.getGryo_data());
        try{
            graph.io(GryoIo.build(GryoVersion.V1_0)).reader().create().readGraph(is, graph);
        }catch (IOException e){
            System.out.println(String.format("matchProjectTest ERROR: pid=%d, msg=%s", pid, e.getMessage()));
            return CompletableFuture.completedFuture( null );
        }

        GraphTraversalSource g = graph.traversal();

        Long source_cnt = new Long(ids.size());
        Long target_cnt = g.V().count().next();
        Long match_cnt = g.V(ids).count().next();
        Long union_cnt = source_cnt + target_cnt - match_cnt;
        // Sync rate = interaction size / union size (%)

        ResponseDto dto = new ResponseDto();
        dto.setState(ResponseDto.StateType.SUCCESS);
        // if( match_cnt.equals(0L) ) dto.setState(ResponseDto.StateType.FAIL);
        String message = String.format("%s[%d]: match=%d, union=%d (source=%d, target=%d)"
                , project.getTitle(), pid, match_cnt, union_cnt, source_cnt, target_cnt);
        dto.setMessage(message);

        try{ g.close(); graph.close(); }catch(Exception e){ System.out.println("matchProjectTest: close error"); }
        return CompletableFuture.completedFuture( dto );
    }

    @Async("agensExecutor")
    public CompletableFuture<GraphDto> loadProject(ClientDto client, Integer pid, Boolean onlyData){

        AgensProject project = agensService.findOneProjectById(pid);
        if( project == null || project.getGryo_data() == null )
            return CompletableFuture.completedFuture( null );

        TinkerGraph graph = graphFactory();                             // 새로 생성후
        final InputStream is = new ByteArrayInputStream(project.getGryo_data());
        try{
            graph.io(GryoIo.build(GryoVersion.V1_0)).reader().create().readGraph(is, graph);
        }catch (IOException e){
            System.out.println(String.format("loadProject ERROR: pid=%d, msg=%s", pid, e.getMessage()));
            return CompletableFuture.completedFuture( null );
        }

        GraphType result = convertGraphType(graph);
        if( result == null ) CompletableFuture.completedFuture( null );

        if( !onlyData ){
            Long gid = graphStorage.addGraph(graph);            // 저장하고 신규 gid 반환
            client.setGid(gid);                                 // client에 배정
        }
        else{
            // 데이터만 가지고 나갈거면 graph 를 close 시킨다
            try{ graph.close(); }catch(Exception e){ System.out.println("loadProject: close error (onlyData)"); }
        }

        GraphDto dto = new GraphDto(client.getGid(), result);
        dto.setState(ResponseDto.StateType.SUCCESS);
        String message = String.format("loadProject[%d]: gid=%d, labels=%d, nodes=%d, edges=%d"
                , pid, client.getGid(), result.getLabels().size(), result.getNodes().size(), result.getEdges().size());
        dto.setMessage(message);

        return CompletableFuture.completedFuture( dto );
    }

    @Async("agensExecutor")
    public CompletableFuture<AgensProject> saveProject(ClientDto client, Long gid, ProjectDto data){
        TinkerGraph graph = graphFactory();
        String result = doSave(graph, data.getGraph());
        if( result == null ) return CompletableFuture.completedFuture( null );

        final OutputStream os = new ByteArrayOutputStream();
        try{
            graph.io(GryoIo.build(GryoVersion.V1_0)).writer().create().writeGraph(os, graph);
        }catch (IOException e){
            System.out.println(String.format("saveProject ERROR: gid=%d, msg=%s", gid, e.getMessage()));
            try{ graph.close();
            }catch(Exception ex){ System.out.println("doSave: graph close error"); }
            return CompletableFuture.completedFuture( null );
        }

        AgensProject project = new AgensProject(client, data);
        project.setGryo_data( ((ByteArrayOutputStream) os).toByteArray() );

        try{ graph.close();
        }catch(Exception e){ System.out.println("saveProject: graph close error"); }
        return CompletableFuture.completedFuture( agensService.saveProject( project ) );
    }

    @Async("agensExecutor")
    public CompletableFuture<ByteArrayOutputStream> exportGraph(String fileType, GraphDto data){
        TinkerGraph graph = graphFactory();
        String result = doSave(graph, data.getGraph());
        if( result == null ) return CompletableFuture.completedFuture( null );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        try{
            if( fileType.equalsIgnoreCase("json") ) {    // graphson
                final GraphSONMapper mapper = graph.io(graphson()).mapper().version(GraphSONVersion.V1_0).normalize(true).create();
                graph.io(graphson()).writer().mapper(mapper).create().writeGraph(os, graph);
            }
            else{                                                       // graphml
                graph.io(IoCore.graphml()).writer().normalize(true).create().writeGraph(os, graph);
            }
        }catch (IOException e){
            System.out.println(String.format("exportGraph ERROR: fileType=%s, msg=%s", fileType, e.getMessage()));
            try{ graph.close();
            }catch(Exception ex){ System.out.println("exportGraph: graph close error"); }
            return CompletableFuture.completedFuture( null );
        }

        try{ graph.close();
        }catch(Exception e){ System.out.println("exportGraph: graph close error"); }
        return CompletableFuture.completedFuture( os );
    }

    @Async("agensExecutor")
    public CompletableFuture<String> updateGraphElement(Long gid, String egrp, String eid, String oper, List<Map<String,Object>> props){
        TinkerGraph graph = graphStorage.getGraph(gid);
        if( graph == null ) return CompletableFuture.completedFuture( null );

        GraphTraversalSource g = graph.traversal();
        Element target = null;
        if( egrp.equals("nodes") && g.V(eid).hasNext() ) target = g.V(eid).next();
        else if( egrp.equals("edges") && g.E(eid).hasNext() ) target = g.E(eid).next();
        if( target == null ) return CompletableFuture.completedFuture( null );

        String result = oper+": ";
        for( Map<String,Object> prop : props ){
            String key = prop.get("key").toString();
            Object value = prop.get("value");
            // String type = prop.get("type").toString();

            if( oper.equals("delete") ){    // delete
                if( target.property(key).isPresent() ) target.property(key).remove();
            }
            else{                           // upsert(save)
                target.property(key,value); // update or insert
            }
            result += "\""+key+"\", ";
        }

        return CompletableFuture.completedFuture( result );
    }

    @Async("agensExecutor")
    public CompletableFuture<String> updateGraph(Long gid, String oper, GraphType data){
        TinkerGraph sGraph = graphStorage.getGraph(gid);
        if( sGraph == null ) return CompletableFuture.completedFuture( null );

        String result = oper+": ";
        if( oper.equals("delete") ){    // delete
            result += doDelete(sGraph, data);
        }
        else{                           // upsert, save
            result += doUpsert(sGraph, data);
        }

        return CompletableFuture.completedFuture( result );
    }

    String doDelete(TinkerGraph graph, GraphType data){
        GraphTraversalSource g = graph.traversal();
        int vcnt = 0, ecnt = 0;

        // vertex id list
        List<String> removeIds = data.getNodes().stream().map(x -> x.getId()).collect(Collectors.toList());
        for(String vid : removeIds){
            if( g.V(vid).hasNext() ){
                // Vertex removed = g.V(vid).drop().next();     // <== drop STEP 이 작동하지 않음!!
                // **NOTE: 메모리DB 특성상 next()로 instance 를 얻어온 다음에 remove 시켜야 반영됨!
                g.V(vid).next().remove();
                vcnt += 1;
            }
        }
        // edge id list
        removeIds = data.getEdges().stream().map(x -> x.getId()).collect(Collectors.toList());
        for(String vid : removeIds){
            if( g.E(vid).hasNext() ){
                g.E(vid).next().remove();
                ecnt += 1;
            }
        }

        try{ g.close();
        }catch(Exception e){ System.out.println("doDelete: GraphTraversalSource close error"); }

        String result = String.format("nodes.size=%d, edges.size=%d => %s", vcnt, ecnt, graph.toString());
        return result;
    }

    String doUpsert(TinkerGraph graph, GraphType data){
        GraphTraversalSource g = graph.traversal();
        int vcnt = 0, ecnt = 0;

        for( NodeType sv : data.getNodes() ){
            Vertex tv = null;
            if( g.V(sv.getId()).hasNext() ){    // update
                tv = g.V(sv.getId()).next();
                tv.property("$$state", "updated");
            }
            else {                              // insert
                tv = graph.addVertex(T.label, sv.getLabel(), T.id, sv.getId());
                tv.property("$$state", "created");
            }

            if( tv != null ){
                copyProperties(tv, sv);       // copy properties: v <- e
                vcnt += 1;
            }
        }

        for( EdgeType se : data.getEdges() ){
            Edge te = null;
            if( g.E(se.getId()).hasNext() ){    // update
                te = g.E(se.getId()).next();
                te.property("$$state", "updated");
            }
            else {                              // insert
                if( g.V( se.getSource() ).hasNext() && g.V( se.getTarget() ).hasNext() ) {
                    te = g.V( se.getSource() ).next().addEdge( se.getLabel(), g.V( se.getTarget() ).next() );
                    te.property("$$state", "created");
                }
            }

            if( te != null ){
                copyProperties(te, se);       // copy properties: v <- e
                ecnt += 1;
            }
        }

        try{ g.close();
        }catch(Exception e){ System.out.println("doUpsert: GraphTraversalSource close error"); }

        String result = String.format("nodes.size=%d, edges.size=%d", vcnt, ecnt);
        return result;
    }

    String doSave(TinkerGraph graph, GraphType data){
        GraphTraversalSource g = graph.traversal();
        int vcnt = 0, ecnt = 0;

        for( NodeType sv : data.getNodes() ){
            Vertex tv = graph.addVertex(T.label, sv.getLabel(), T.id, sv.getId().toString());
            if( tv != null ){
                tv.property("$$state", "saved");
                copyProperties(tv, sv);       // copy properties: v <- e
                vcnt += 1;
            }
        }
        for( EdgeType se : data.getEdges() ){
            if( g.V( se.getSource() ).hasNext() && g.V( se.getTarget() ).hasNext() ) {
                Vertex outV = g.V( se.getSource() ).next();
                Vertex inV = g.V( se.getTarget() ).next();
                Edge te = outV.addEdge( se.getLabel(), inV, T.id, se.getId().toString() );
                if( te != null ){
                    te.property("$$state", "saved");
                    copyProperties(te, se);       // copy properties: v <- e
                    ecnt += 1;
                }
            }
        }

        try{ g.close();
        }catch(Exception e){ System.out.println("doSave: GraphTraversalSource close error"); }

        String result = String.format("nodes.size=%d, edges.size=%d", vcnt, ecnt);
        return result;
    }

    /////////////////////////////////////////////////////////////////////////

    @Async("agensExecutor")
    public CompletableFuture<Boolean> deleteGraph(Long gid){
        return CompletableFuture.completedFuture( graphStorage.removeGraph(gid) );
    }

    @Async("agensExecutor")
    public CompletableFuture<Boolean> writeGraph(Long gid){
        TinkerGraph graph = graphStorage.getGraph(gid);
        if( graph == null ) return CompletableFuture.completedFuture( false );

        String filename = String.format("%s/gid_%d.json", this.fileStorageService.getDownloadPath(), gid);
        try (final OutputStream os = new FileOutputStream(filename)) {
            final GraphSONMapper mapper = graph.io(graphson()).mapper().version(GraphSONVersion.V1_0).normalize(true).create();
            graph.io(graphson()).writer().mapper(mapper).create().writeGraph(os, graph);
        }catch (IOException e){
            System.out.println(String.format("writeGraph ERROR: gid=%d, msg=%s", gid, e.getMessage()));
            return CompletableFuture.completedFuture( false );
        }

        return CompletableFuture.completedFuture( true );
    }

    @Async("agensExecutor")
    public CompletableFuture<List<List<Object>>> findShortestPath(Long gid, String sid, String eid) throws InterruptedException {
        if( !graphStorage.hasGraph(gid) ) return CompletableFuture.completedFuture( null );
        List<List<Object>> result = new ArrayList<List<Object>>();

        TinkerGraph graph = graphStorage.getGraph(gid);
        GraphTraversalSource g = graph.traversal();

        // 출처 http://tinkerpop.apache.org/docs/current/recipes/#shortest-path
        List<Path> paths = g.V(sid).repeat(__.out().simplePath()).until(__.hasId(eid)).path().toList();
        paths.forEach(p -> {
            List<Object> row = new ArrayList<Object>();
            p.forEach(v -> {
                row.add( ((Element)v).id() );
            });
            result.add( row );
        });

        try{ g.close(); }catch(Exception e){ System.out.println("findShortestPath: close error, gid="+gid); }
        return CompletableFuture.completedFuture( result );
    }

    // **NOTE: node, edge 각 50개만 되어도 3~4분 소요. 그 이상은 거의 무응답이라 못씀!!
    @Async("agensExecutor")
    public CompletableFuture<List<List<Object>>> findConnectedGroup(Long gid) throws InterruptedException {
        if( !graphStorage.hasGraph(gid) ) return CompletableFuture.completedFuture( null );
        List<List<Object>> result = new ArrayList<List<Object>>();

        TinkerGraph graph = graphStorage.getGraph(gid);
        GraphTraversalSource g = graph.traversal();

        // 출처 http://tinkerpop.apache.org/docs/current/recipes/#connected-components
        List<List<Object>> groups = g.V().emit(__.cyclicPath().or().not(__.both()))
                .repeat(__.both()).until(__.cyclicPath())
                .path().aggregate("p").unfold().dedup().map(__.as("v").select("p").unfold()
                        .filter(__.unfold().where(P.eq("v"))).unfold().dedup().order().by(T.id).fold())
                .dedup().toList();
        groups.forEach(grp -> {
            List<Object> row = new ArrayList<Object>();
            grp.forEach(v -> {
                row.add( ((Element)v).id() );
            });
            result.add( row );
        });

        try{ g.close(); }catch(Exception e){ System.out.println("findConnectedGroup: close error, gid="+gid); }
        return CompletableFuture.completedFuture( result );
    }

    // **NOTE: 1000 개 수준에서 거의 즉시 응답 (만일 느려지면 시작점 hasLabel 추가할 것)
    @Async("agensExecutor")
    public CompletableFuture<List<List<Object>>> findCycleDetection(Long gid) throws InterruptedException {
        if( !graphStorage.hasGraph(gid) ) return CompletableFuture.completedFuture( null );
        List<List<Object>> result = new ArrayList<List<Object>>();

        TinkerGraph graph = graphStorage.getGraph(gid);
        GraphTraversalSource g = graph.traversal();

        // 출처 http://tinkerpop.apache.org/docs/current/recipes/#cycle-detection
        List<Path> paths = g.V().as("a").repeat(__.out().simplePath())
                .emit( __.loops().is(P.gt(1)).is(P.lte(6)) ).out().where( P.eq( "a" ) ).path().dedup()
                .by(__.unfold().order().by(T.id).dedup().fold()).toList();
        paths.forEach(grp -> {
            List<Object> row = new ArrayList<Object>();
            grp.forEach(v -> {
                row.add( ((Element)v).id() );
            });
            result.add( row );
        });

        try{ g.close(); }catch(Exception e){ System.out.println("findConnectedGroup: close error, gid="+gid); }
        return CompletableFuture.completedFuture( result );
    }

    ///////////////////////////////////////////////////////

    @Async("agensExecutor")
    public CompletableFuture<GraphDto> importGraphFile(Long gid, MultipartFile file, String fileExt) throws InterruptedException {

        final TinkerGraph sgraph = graphStorage.getGraph(gid);
        if( sgraph == null ) return CompletableFuture.completedFuture( null );

        String filePath = this.fileStorageService.importFile(file);
        if( filePath == null ) return CompletableFuture.completedFuture( null );

        final TinkerGraph tgraph = graphFactory();
        if( fileExt.equals(".json") || fileExt.equals(".graphson") ){
            try{
                tgraph.io(IoCore.graphson()).readGraph(filePath);
            }catch (IOException ie){
                System.out.println("Error on read Graphson file: "+ie.getMessage());
                return CompletableFuture.completedFuture( null );
            }
        }
        else if( fileExt.equals(".xml") || fileExt.equals(".graphml") ){
            try{
                tgraph.io(IoCore.graphml()).readGraph(filePath);
            }catch (IOException ie){
                System.out.println("Error on read Graphml file: "+ie.getMessage());
                return CompletableFuture.completedFuture( null );
            }
        }

        System.out.println( "==> "+mergeGraph(sgraph, tgraph) );
        try{ tgraph.close(); }catch(Exception e){
            System.out.println("importGraphFile: tgraph close error, gid="+gid); }

        GraphType result = convertGraphType(sgraph);
        if( result == null ) CompletableFuture.completedFuture( null );

        GraphDto dto = new GraphDto(gid, result);
        dto.setState(ResponseDto.StateType.SUCCESS);
        String message = String.format("import & merge: gid=%d, labels=%d, nodes=%d, edges=%d"
                , gid, result.getLabels().size(), result.getNodes().size(), result.getEdges().size());
        dto.setMessage(message);

        return CompletableFuture.completedFuture( dto );
    }

    /////////////////////////////////////////////////////////////////////////

    // commons-math3 기술통계
    // ** 참고문서
    // http://commons.apache.org/proper/commons-math/userguide/stat.html
    // https://www.baeldung.com/apache-commons-frequency
    // https://stackoverflow.com/a/11109520/6811653
    // https://github.com/obiba/magma/blob/master/magma-math/src/main/java/org/obiba/magma/math/summary/TextVariableSummary.java

    @Async("agensExecutor")
    public CompletableFuture<PropStatDto> getPropStats(
            Long gid, String group, String label, String prop, Integer binCount
    ) throws InterruptedException {
        PropStatDto result = new PropStatDto(gid, label, prop);             // result
        result.setState(ResponseDto.StateType.FAIL);
        result.setMessage(String.format("gid<%d>: graph does not exist", gid, label, prop));
        if( !graphStorage.hasGraph(gid) ) return CompletableFuture.completedFuture( result );

        TinkerGraph graph = graphStorage.getGraph(gid);
        GraphTraversalSource g = graph.traversal();

        List<Object> props = group.equals("nodes")
                ? g.V().hasLabel(label).values(prop).toList()
                : g.E().hasLabel(label).values(prop).toList();
        if( props.size() == 0 ){
            result.setMessage(String.format("gid<%d>: %s[%s] is empty", gid, label, prop));
            return CompletableFuture.completedFuture( result );
        }

        DescriptiveStatistics statistics = new DescriptiveStatistics();
        Frequency frequency = new Frequency();
        List<PropStatDto.PropFreq> frequencies = new ArrayList<>();

        String propType = JsonbUtil.simpleTypeof(props.get(0));
        switch( propType ) {
            case "NUMBER":
                result.setType(propType);
                props.forEach(d -> {
                    if(JsonbUtil.simpleTypeof(d).equals(propType))
                        statistics.addValue(Double.parseDouble(d.toString()));
                });
                result.setStat(statistics);

                EmpiricalDistribution distribution = new EmpiricalDistribution(binCount);
                Double[] data = props.stream().map(d -> {
                    if( JsonbUtil.simpleTypeof(d).equals(propType) )
                        return Double.parseDouble(d.toString());
                    else return null;
                }).filter(d -> d != null).toArray(Double[]::new);
                distribution.load( ArrayUtils.toPrimitive(data) );

                double[] bounds = distribution.getUpperBounds();
                List<SummaryStatistics> binStats = distribution.getBinStats();
                for( int j=0; j<bounds.length; j++){
                    frequencies.add(new PropStatDto.PropFreq(Double.toString(bounds[j]),
                            binStats.get(j).getN(), binStats.get(j).getN() == 0 ));
                }
                // frequencies.stream().forEach(x -> System.out.println(String.format("%s %d", x.getValue(), x.getFreq())));
                result.setRows(frequencies);

                result.setState(ResponseDto.StateType.SUCCESS);
                result.setMessage(String.format("gid<%d>: %s[%s](%s) frequencies.size=%d, props.size=%d",
                        gid, label, prop, propType, frequencies.size(), props.size()));
                break;

            case "BOOLEAN":
            case "STRING":
                result.setType(propType);
                props.forEach(d -> {
                    if(JsonbUtil.simpleTypeof(d).equals(propType))
                        frequency.addValue(d.toString());
                });
                Iterator<String> concat = Iterators.transform(frequency.valuesIterator(), new Function<Comparable<?>, String>() {
                    @Override
                    public String apply(Comparable<?> input) {
                        return input.toString();
                    }
                });
                while(concat.hasNext()) {
                    String value = concat.next();
                    frequencies.add( new PropStatDto.PropFreq(value, frequency.getCount(value),
                            value.equals("N/A")));
                }

                Collections.sort(frequencies, new Comparator<PropStatDto.PropFreq>() {
                    @Override
                    public int compare(PropStatDto.PropFreq o1, PropStatDto.PropFreq o2) {
                        return (int) (o2.getFreq() - o1.getFreq());
                    }
                });
                // frequencies.stream().forEach(x -> System.out.println(String.format("%s %d", x.getValue(), x.getFreq())));
                result.setRows(frequencies);

                frequencies.forEach(d -> statistics.addValue( (double) d.getFreq() ));
                result.setStat(statistics);

                result.setState(ResponseDto.StateType.SUCCESS);
                result.setMessage(String.format("gid<%d>: %s[%s](%s) frequencies.size=%d, props.size=%d",
                        gid, label, prop, propType, frequencies.size(), props.size()));
                break;

            default:
                result.setType(propType);
                result.setState(ResponseDto.StateType.FAIL);
                result.setMessage(String.format("gid<%d>: %s[%s] is not proper type. (%s)", gid, label, prop, propType));
        }

        try{ g.close(); }catch(Exception e){ System.out.println("PropStat: close error, gid="+gid); }
        return CompletableFuture.completedFuture( result );
    }


/*
http://localhost:8085/api/graph/propstat/1001?group=nodes&label=order&prop=freight

Descriptive ==> DescriptiveStatistics:
n: 66
min: 0.17
max: 810.05
mean: 86.4609090909091
std dev: 120.10468642271749
median: 52.385
skewness: 4.06963494100738
kurtosis: 21.35787960947871

frequency ==> Value 	 Freq. 	 Pct. 	 Cum Pct.
0.17	1	2%	2%
0.75	1	2%	3%
0.78	1	2%	5%
1.17	1	2%	6%
1.39	1	2%	8%
1.85	1	2%	9%
3.67	1	2%	11%
... <=== 모두 1의 크기를 가지므로 이런식으론 쓸모 없음

yData ==> [0, 8, 8, 1, 0, 1, 1, 6, 3, 2, 1, 5, 0, 1, 0, 1, 3, 8, 0, 1, 6, 4, 2, 2, 0, 1, 1]
xData ==> [0-0, 0-10, 10-20, 100-110, 110-120, 120-130, 130-140, 140-150, 150-160, 160-170,
            170-180, 20-30, 210-220, 220-230, 230-240, 240-250, 30-40, 40-50, 470-480, 480-490,
            50-60, 60-70, 70-80, 80-90, 800-810, 810-820, 90-100
            ]
 */
}
