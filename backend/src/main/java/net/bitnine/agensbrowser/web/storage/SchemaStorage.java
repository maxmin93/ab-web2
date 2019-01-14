package net.bitnine.agensbrowser.web.storage;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.bitnine.agensbrowser.web.message.LabelDto;
import net.bitnine.agensbrowser.web.message.SchemaDto;
import net.bitnine.agensbrowser.web.message.RequestDto;
import net.bitnine.agensbrowser.web.message.ResponseDto;
import net.bitnine.agensbrowser.web.persistence.outer.model.EdgeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
import net.bitnine.agensbrowser.web.persistence.outer.model.HealthType;
import net.bitnine.agensbrowser.web.persistence.outer.model.NodeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.DatasourceType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Scope("singleton")
public class SchemaStorage {

    private static final Logger logger = LoggerFactory.getLogger(SchemaStorage.class);

    private Long txSeq;             // 스케줄링에 의한 작업 회수
    private String progressMessage; // 스케줄링의 작업단계 설명

    private Boolean isDirty;        // whether data is changed or not for Source
    private Boolean isDirtyCopy;    // whether data is changed or not for Copy

    private float version;          // DB version : 1.3 or under
    private HealthType health;

    // background로 진행되는 데이터셋 ==> 업데이트가 완료되면 Copy셋으로 복사
    private DatasourceType graph;
    private Set<LabelType> labels;
    private GraphType meta;

    // front 요청에 대응하는 데이터셋 (Copy셋)
    private DatasourceType graphCopy;
    private Set<LabelType> labelsCopy;
    private GraphType metaCopy;

    public SchemaStorage() {
        super();
        this.txSeq = -1L;   // 최초 시작은 0부터
        this.progressMessage = "";

        this.version = 1.3f;
        this.health = new HealthType();

//        ConcurrentHashMap labelsMap = new ConcurrentHashMap<LabelType, Object>();
//        this.labels = labelsMap.newKeySet();
        this.labels = new HashSet<LabelType>();
        this.clear();

        isDirtyCopy = true;
        this.graphCopy = new DatasourceType();
        this.metaCopy = new GraphType();

//        ConcurrentHashMap labelsCopyMap = new ConcurrentHashMap<LabelType, Object>();
//        this.labelsCopy = labelsCopyMap.newKeySet();
        this.labelsCopy = new HashSet<LabelType>();
    }

    public void clear() {
        isDirty = true;
        graph = new DatasourceType();
        if( labels != null ) labels.clear();
        meta = new GraphType();
    }

    public float getVersion() { return version; }
    public void setVersion(float version) { this.version = version; }
    public void setHealthInfo(HealthType health) { this.health = health; }

    // 이상없이 Copy 되었으면 true, 아니면 false 리턴
    public boolean storageCopy(Boolean forced) {
        // Dirty 상태이면 Copy 하지 않는다
        // But, forced == true 이면 그냥 실행
        if( isDirty == true && forced != true ) return false;

        boolean copied = true;
        try {
            graphCopy = (DatasourceType) graph.clone();
        }catch (CloneNotSupportedException e){
            System.out.println("storageCopy: FAIL<DatasourceType> => "+e.getCause());
            graphCopy = new DatasourceType();
            copied = false;
        }
        try {
            labelsCopy.clear();
            for( Iterator<LabelType> iter = labels.iterator(); iter.hasNext(); ){
                LabelType clone = (LabelType) iter.next().clone();
                labelsCopy.add(clone);
            }
        }catch (CloneNotSupportedException e){
            System.out.println("storageCopy: FAIL<LabelTypes> => "+e.getCause());
            labelsCopy = new HashSet<LabelType>();
            copied = false;
        }
        try {
            metaCopy = (GraphType) meta.clone();
        }catch (CloneNotSupportedException e){
            System.out.println("storageCopy: FAIL<MetaGraph> => "+e.getCause());
            metaCopy = new GraphType();
            copied = false;
        }

        return copied;
    }

    public Long getTxSeq() { return this.txSeq; }
    public String getProgressMessage() { return this.progressMessage; }

    public Boolean getIsDirty() { return isDirty; }
    public DatasourceType getGraphType() {
        return graph;
    }
    public List<LabelType> getLabelTypes() { return Lists.newArrayList(labels); }   // using Guava
    public GraphType getMetaGraph() { return meta; }
    public HealthType getHealthInfo() { return health; }

    public Boolean getIsDirtyCopy() { return isDirtyCopy; }
    public DatasourceType getGraphTypeCopy() { return graphCopy; }
    public List<LabelType> getLabelTypesCopy() { return Lists.newArrayList(labelsCopy); }   // using Guava
    public GraphType getMetaGraphCopy() { return metaCopy; }

    public int getNodesSize(){
        int size = 0;
        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();){
            LabelType labelType = iter.next();
            if( labelType.getType().equals("nodes") ) size += 1;
        }
        return size;
    }
    public int getEdgesSize(){
        int size = 0;
        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();){
            LabelType labelType = iter.next();
            if( labelType.getType().equals("edges") ) size += 1;
        }
        return size;
    }
    public int getNodesCopySize(){
        int size = 0;
        for(Iterator<LabelType> iter = labelsCopy.iterator(); iter.hasNext();){
            LabelType labelType = iter.next();
            if( labelType.getType().equals("nodes") ) size += 1;
        }
        return size;
    }
    public int getEdgesCopySize(){
        int size = 0;
        for(Iterator<LabelType> iter = labelsCopy.iterator(); iter.hasNext();){
            LabelType labelType = iter.next();
            if( labelType.getType().equals("edges") ) size += 1;
        }
        return size;
    }

    public LabelType findLabelType(String oid){
        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();){
            LabelType labelType = iter.next();
            if( labelType.getId().equals(oid) ) return labelType;
        }
        return null;
    }
    public LabelType findLabelType(String type, String name){
        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();){
            LabelType labelType = iter.next();
            if( labelType.getType().equals(type) && labelType.getName().equals(name) )
                return labelType;
        }
        return null;
    }
//    public LabelType findLabelTypeByTypeAndName(LabelType.ElemType type, String name){
//        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();){
//            LabelType labelType = iter.next();
//            if( labelType.getType().equals(type) && labelType.getName().equals(name) )
//                return labelType;
//        }
//        return null;
//    }

    public LabelType findLabelTypeCopy(String oid){
        for(Iterator<LabelType> iter = labelsCopy.iterator(); iter.hasNext();){
            LabelType labelType = iter.next();
            if( labelType.getId().equals(oid) ) return labelType;
        }
        return null;
    }
    public LabelType findLabelTypeCopy(String type, String name){
        for(Iterator<LabelType> iter = labelsCopy.iterator(); iter.hasNext();){
            LabelType labelType = iter.next();
            if( labelType.getType().equals(type) && labelType.getName().equals(name) )
                return labelType;
        }
        return null;
    }
//    public LabelType findLabelTypeCopyByTypeAndName(LabelType.ElemType type, String name){
//        for(Iterator<LabelType> iter = labelsCopy.iterator(); iter.hasNext();){
//            LabelType labelType = iter.next();
//            if( labelType.getType().equals(type) && labelType.getName().equals(name) )
//                return labelType;
//        }
//        return null;
//    }

    public void setTxSeq(Long txSeq) { this.txSeq = txSeq; }
    public void setProgressMessage(String message) { this.progressMessage = message; }

    public void setIsDirty(Boolean isDirty) { this.isDirty = isDirty; }
    public void setGraphType(DatasourceType graph){
        this.graph = graph;
        this.graph.setVersion(this.version);
    }
    public void setLabelTypes(List<LabelType> labels) { this.labels = Sets.newHashSet(labels); }    // using Guava
    public void setMetaGraph(GraphType meta) { this.meta = meta; }

    public void setIsDirtyCopy(Boolean isDirty) { this.isDirtyCopy = isDirty; }

/*
    public void addLabelType(LabelType label) {
        // SET 특성상 기존꺼가 있으면 지우고 새걸로 넣어야 함 (replace)
        if( !this.labels.add(label) ){
            this.labels.remove(label);
            this.labels.add(label);
            // 기존 meta graph에 있던 node 제거
        };
        // node인 경우에 meta graph 에도 추가 (edge는 관계 없이는 추가 불가)
        if( label.getType().equals(LabelType.ElemType.NODE) ) {
            this.meta.getNodes().add(new NodeType(label));
        }
    }
    public void removeLabelType(LabelType label) {
        // label 리스트에서 지우고
        this.labels.remove(label);
        // meta 그래프의 node 혹은 edge에서도 지우고
        // if label is NODE, related EDGEs also are removed from META GRAPH
        if( label.getType().equals(LabelType.ElemType.NODE) ){
            for(Iterator<NodeType> iter = meta.getNodes().iterator(); iter.hasNext(); ) {
                NodeType node = iter.next();
                if( node.getId().equals(label.getOid()) ) iter.remove();
            }
            for(Iterator<EdgeType> iter = meta.getEdges().iterator(); iter.hasNext(); ){
                EdgeType edge = iter.next();
                // edge의 source 또는 target에 연결되어 있다면 edge 삭제
                if( edge.getSource().equals(label.getOid()) || edge.getTarget().equals(label.getOid()) ){
                    iter.remove();
                }
            }
        }
        else{
            // edge는 자기 자신만 지우면 된다
            for(Iterator<EdgeType> iter = meta.getEdges().iterator(); iter.hasNext(); ) {
                EdgeType edge = iter.next();
                if( edge.getId().equals(label.getOid()) ) iter.remove();
            }
        }
    }
*/
    // labelsCopy 에 추가하고 metaGraph 에도 추가
    public void addLabelTypeCopy(LabelType label) {
        // SET 특성상 기존꺼가 있으면 지우고 새걸로 넣어야 함 (replace)
        if( !this.labelsCopy.add(label) ){
            this.labelsCopy.remove(label);
            this.labelsCopy.add(label);
            // 기존 meta graph에 있던 node 제거
        };
        // node인 경우에 meta graph 에도 추가 (edge는 관계 없이는 추가 불가)
        if( label.getType().equals("nodes") ) {
            this.metaCopy.getNodes().add(new NodeType(label));
        }
    }

    // 갱신된 label 의 내용으로 metaGraph 의 node 또는 edge 에 업데이트
    public void updateLabelTypeCopy(LabelType label, String requestType) {
        if( label.getType().equals("nodes") ){
            for(Iterator<NodeType> iter = metaCopy.getNodes().iterator(); iter.hasNext(); ) {
                NodeType node = iter.next();
                if( node.getId().equals(label.getId()) ){
                    if( requestType.equals(RequestDto.RequestType.RENAME.toString()) )
                        node.setProperty("name",label.getName());
                    else if( requestType.equals(RequestDto.RequestType.COMMENT.toString()) )
                        node.setProperty("desc",label.getDesc());
                    node.setProperty("is_dirty", true);
                }
            }
        }
        else{
            for(Iterator<EdgeType> iter = metaCopy.getEdges().iterator(); iter.hasNext(); ) {
                EdgeType edge = iter.next();
                if( edge.getId().equals(label.getId()) ){
                    if( requestType.equals(RequestDto.RequestType.RENAME.toString()) )
                        edge.setProperty("name", label.getName());
                    else if( requestType.equals(RequestDto.RequestType.COMMENT.toString()) )
                        edge.setProperty("desc", label.getDesc());
                    edge.setProperty("is_dirty", true);
                }
            }
        }
    }

    // labelsCopy 에서 지우고, metaGraph 에서도 삭제
    public void removeLabelTypeCopy(LabelType label) {
        // label 리스트에서 지우고
        this.labelsCopy.remove(label);
        // meta 그래프의 node 혹은 edge에서도 지우고
        // if label is NODE, related EDGEs also are removed from META GRAPH
        if( label.getType().equals("nodes") ){
            for(Iterator<NodeType> iter = metaCopy.getNodes().iterator(); iter.hasNext(); ) {
                NodeType node = iter.next();
                if( node.getId().equals(label.getId()) ) iter.remove();
            }
            for(Iterator<EdgeType> iter = metaCopy.getEdges().iterator(); iter.hasNext(); ){
                EdgeType edge = iter.next();
                // edge의 source 또는 target에 연결되어 있다면 edge 삭제
                if( edge.getSource().equals(label.getId()) || edge.getTarget().equals(label.getId()) ){
                    iter.remove();
                }
            }
        }
        else{
            // edge는 자기 자신만 지우면 된다
            for(Iterator<EdgeType> iter = metaCopy.getEdges().iterator(); iter.hasNext(); ) {
                EdgeType edge = iter.next();
                if( edge.getId().equals(label.getId()) ) iter.remove();
            }
        }
    }

    public LabelDto getLabelDto(RequestDto req) {
        String type = req.getCommand().equals("elabel") ? "edges" : "nodes";
        // if not exists, label is null
        LabelType label = findLabelType(type, req.getTarget());
        return new LabelDto(req, label);
    }
    public LabelDto getLabelDtoCopy(RequestDto req) {
        String type = req.getCommand().equals("elabel") ? "edges" : "nodes";
        // if not exists, label is null
        LabelType label = findLabelTypeCopy(type, req.getTarget());
        return new LabelDto(req, label);
    }
/*
    @Async
    public CompletableFuture<SchemaDto> getSchemaDto() {
        // count nodes and edges
        int nodes_size = getNodesSize();
        int edges_size = getEdgesSize();

        // for INFO
        String message = String.format("graph='%s', labels.size=%d (%d/%d), relations=%d, isDirty=%b"
                , graph.getName(), labels.size(), nodes_size, edges_size, meta.getEdges().size(), isDirty);

        SchemaDto dto = new SchemaDto(isDirty, graph, this.getLabelTypes(), meta);
        dto.setMessage(message);
        return CompletableFuture.completedFuture(dto);
    }
*/
    public SchemaDto getSchemaDto() {
        // count nodes and edges
        int nodes_size = getNodesSize();
        int edges_size = getEdgesSize();

        // for INFO
        String message = String.format("%s, labels.size=%d (%d/%d), relations=%d, isDirty=%b"
                , graph.getName(), labels.size(), nodes_size, edges_size, meta.getEdges().size(), isDirty);

        SchemaDto dto = new SchemaDto(isDirty, graph, this.getLabelTypes(), meta);
        dto.setMessage(message);

        // 최초 meta storage 갱신시에는 PENDING 상태를 알린다 (기다리던지..)
        if( this.txSeq < 0L ){
            dto.setState(ResponseDto.StateType.PENDING);
            dto.setMessage(this.progressMessage);
        }
        else dto.setState(ResponseDto.StateType.SUCCESS);

        return dto;
    }
    public SchemaDto getSchemaDtoCopy() {
        // count nodes and edges
        int nodes_size = getNodesCopySize();
        int edges_size = getEdgesCopySize();

        // for INFO
        String message = String.format("%s, labels.size=%d (%d/%d), relations=%d, isDirty=%b"
                , graphCopy.getName(), labelsCopy.size(), nodes_size, edges_size, metaCopy.getEdges().size(), isDirtyCopy);

        SchemaDto dto = new SchemaDto(isDirtyCopy, graphCopy, this.getLabelTypesCopy(), metaCopy);
        dto.setMessage(message);

        // 최초 meta storage 갱신시에는 PENDING 상태를 알린다 (기다리던지..)
        if( this.txSeq < 0L ){
            dto.setState(ResponseDto.StateType.PENDING);
            dto.setMessage(this.progressMessage);
        }
        else dto.setState(ResponseDto.StateType.SUCCESS);

        return dto;
    }

}
