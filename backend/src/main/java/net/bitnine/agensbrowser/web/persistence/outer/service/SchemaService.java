package net.bitnine.agensbrowser.web.persistence.outer.service;

import net.bitnine.agensbrowser.web.config.properties.AgensProductProperties;
import net.bitnine.agensbrowser.web.message.RequestDto;
import net.bitnine.agensbrowser.web.message.ResponseDto;
import net.bitnine.agensbrowser.web.persistence.outer.dao.SchemaDao;
import net.bitnine.agensbrowser.web.persistence.outer.model.EdgeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
import net.bitnine.agensbrowser.web.persistence.outer.model.HealthType;
import net.bitnine.agensbrowser.web.persistence.outer.model.NodeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.DatasourceType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.PgprocType;
import net.bitnine.agensbrowser.web.storage.SchemaStorage;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@Service
@Transactional
public class SchemaService {

    private SchemaDao dao;
    private SchemaStorage storage;

    private AgensProductProperties productProperties;

    @Autowired
    public SchemaService(
            SchemaDao dao,
            SchemaStorage storage,
            AgensProductProperties productProperties
    ){
        super();
        this.dao = dao;
        this.storage = storage;
        this.productProperties = productProperties;
    }

    public SchemaStorage getStorage(){ return this.storage; }

    // determine whether AgensGraph is over 1.3 or not
    public String checkAGVersion(){
        float version = dao.checkAGVersion();
        storage.setVersion(version);

        if( version >= 1.3f ) return "v1.3 or over";
        return "v1.2 or under";
    }

    public void updateHealthInfo(){
        HealthType healthInfo = dao.getHealthTest();
        healthInfo.setProductName( productProperties.getName() );
        healthInfo.setProductVersion( productProperties.getVersion() );
        storage.setHealthInfo( healthInfo );
    }
    public HealthType getHealthInfo(){ return storage.getHealthInfo(); }

    // label 카운트 세기
    public int loadLabelCounts(List<LabelType> labels) throws SQLException {

        int countOfValue = 0;
        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();) {
            LabelType label = iter.next();
            Long count = 0L;
            try {
                count = dao.getLabelCount(label.getName());
            }catch( SQLException e ){
                throw new PSQLException("loadLabelCounts() failed.", PSQLState.CONNECTION_FAILURE, e);
            }
            if( count >= 0) {
                label.setSize(count);
                countOfValue += 1;
            }
            else label.setIsDirty(true);       // -1 이면 비정상
        }
        return countOfValue;
    }

    /*
        // property를 가진 label 카운트 세기
        public int loadLabelProperties(List<LabelType> labels) throws SQLException {

            int countWithProperties = 0;
            for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();) {
                LabelType label = iter.next();
                if( label.getSizeNotEmpty().equals(0L) ) continue;
                countWithProperties += 1;

                // property 항목 다시 체크 안해도 되는 경우
                // 1) 기존 LabelType의 NotEmpty카운트와 같으면서
                // 2) 기존 LabelType이 dirty 상태가 아닌경우
                // ** 변동이 없으면 체크하지 않는걸로 (시간 오래 걸리고 DB에 부담)
                LabelType prev = storage.findLabelType(label.getId());
                if( prev != null )
                    if( prev.getSizeNotEmpty().equals(label.getSizeNotEmpty()) && prev.getIsDirty() == false ){
                        label.setProperties(prev.getProperties());
                        label.setIsDirty(false);
                        continue;
                    }

                // 그 외에는 properties 체크
                List<PropertyType> properties = null;
                try {
                    properties = dao.getLabelProperties(label.getName());
                }catch( SQLException e ){
                    throw new PSQLException("loadLabelProperties() failed.", PSQLState.CONNECTION_FAILURE, e);
                }

                if( properties != null ) {
                    label.setProperties(properties);
                    label.setIsDirty(false);
                }
                else{
                    // properties가 있는데도 못읽어온 경우: 비정상
                    if( prev != null ) label.setProperties(prev.getProperties());
                    label.setIsDirty(true);
                }

                // for INFO
                System.out.println(String.format(" + %s: label.size=%d, properties.size=%d, isDirty=%b",
                        label.getName(), label.getSize(), label.getProperties().size(), label.getIsDirty()));
            }
            return countWithProperties;
        }
    */
    public Boolean checkLabelsDirty(List<LabelType> labels){
        Boolean totalDirty = false;
        for(Iterator<LabelType> iter = labels.iterator(); iter.hasNext();) {
            LabelType label = iter.next();
            totalDirty = totalDirty | label.getIsDirty();
        }
        return totalDirty;
    }

    public int loadMetaGraph(GraphType meta, List<LabelType> labels) throws SQLException {

        Set<LabelType> metaLabels = meta.getLabels(); metaLabels.clear();
        Set<NodeType> nodes = meta.getNodes();  nodes.clear();
        Set<EdgeType> edges = meta.getEdges();  edges.clear();
        int countOfVertex = 0, countOfVertexWithData = 0;
        int countOfRelation = 0, countOfRelationWithData = 0;

        for( Iterator<LabelType> iter = labels.iterator(); iter.hasNext(); ){
            LabelType label = iter.next();

            if( label.getType().equals("nodes") ) {
                nodes.add(new NodeType(label));

                countOfVertex += 1;
                if( !label.getSize().equals(0L) ) countOfVertexWithData += 1;
            }
            else if( label.getType().equals("edges") ){

                try{
                    List<Long[]> relations = dao.getRelationCount(label.getName());
                    for( Iterator<Long[]> rel = relations.iterator(); rel.hasNext(); ){
                        //
                        // ** NOTE: AgensGraph의 oid는 int 형태의 숫자기 때문에 이런 처리가 가능한거다!!
                        // ==> record = { edge, source, target, relationCnt };
                        Long[] record = rel.next();
                        if( record.length == 4 && String.valueOf(record[0]).equals(label.getId())
                                && ( !record[1].equals(0L) && !record[2].equals(0L) )){
                            // for DEBUG
                            // System.out.println(String.format("edge[%d]: (%d)->(%d) = %d", record[0], record[1], record[2], record[3]));
                            
                            // neighbors 에 labelName 추가
                            LabelType sourceLabel = storage.findLabelType( String.valueOf(record[1]) );
                            LabelType targetLabel = storage.findLabelType( String.valueOf(record[2]) );
                            if( sourceLabel != null && targetLabel != null ){
                                // source와 target이 온전한 edge 인 경우만 등록
                                EdgeType edge = new EdgeType(label, String.valueOf(record[1]), String.valueOf(record[2]));
                                edge.setSize(record[3]);    // edge의 전체 개수와 source/target에 사용된 개수는 다르다!
                                edges.add( edge );

                                // edge label에 source_neighbors와 target_neighbors에 등록
                                label.getSourceNeighbors().add( sourceLabel.getName() );
                                label.getTargetNeighbors().add( targetLabel.getName() );
                                // 연결된 node label에도 source와 target neighbors로 등록
                                sourceLabel.getTargetNeighbors().add( targetLabel.getName() );
                                targetLabel.getSourceNeighbors().add( sourceLabel.getName() );
                            }
                        }
                    }
                }catch( SQLException e ){
                    throw new PSQLException("loadMetaGraph() failed.", PSQLState.CONNECTION_FAILURE, e);
                }

                countOfRelation += 1;
                if( !label.getSize().equals(0L) ) countOfRelationWithData += 1;
            }
        }

        // metaLabels에 NodeType, Edge를 의미하는 LabelType 을 임의로 만들어 넣기 (카운트 저장용)
        // : LabelType(String oid, String name, String type, String owner, String desc)
        LabelType metaNodeType = new LabelType("NODE", "nodes");
        metaNodeType.setSize(new Long(countOfVertex));                      // 전체 NodeType 카운트
//        metaNodeType.setSizeNotEmpty(new Long(countOfVertexWithData));      // 데이터가 있는 NodeType 카운트
        metaLabels.add(metaNodeType);
        LabelType metaEdgeType = new LabelType("EDGE", "edges");
        metaEdgeType.setSize(new Long(countOfRelation));                    // 전체 EdgeType 카운트
//        metaEdgeType.setSizeNotEmpty(new Long(countOfRelationWithData));    // 데이터가 있는 EdgeType 카운트
        metaLabels.add(metaEdgeType);

        return countOfRelation;
    }

    public void reloadMeta(Long txSeq){

        // graph_path 정보 읽어오기
        storage.setProgressMessage(String.format("reload[%03d]: Just begining. read DB info... 3",txSeq)+"%");
        DatasourceType graph = dao.getGraphType();
        if( graph != null ) storage.setGraphType( graph );

        Boolean totalDirty = storage.getGraphType().getIsDirty();
        int countOfValue = -1, countOfRelation= -1;

        try{
            // label 리스트 읽어오기
            storage.setProgressMessage(String.format("reload[%03d]: read Labels LIST.. 11",txSeq)+"%");
            List<LabelType> labels = dao.getLabelTypes();
            if( labels != null ){
                // 최초 실행시인 경우 일단 LabelTypes 정보 채워넣기
                if( storage.getLabelTypes().size() == 0 ) storage.setLabelTypes( labels );
                storage.setProgressMessage(String.format("reload[%03d]: read counts of Labels LIST(%d).. 29",txSeq, labels.size())+"%");
                // label 카운트 : size (**NOTE: pg_stat_user_tables의 값을 믿을 수 없음!!)
                countOfValue = loadLabelCounts( labels );
/*
                // label의 properties 카운트 : size, getSizeNotEmpty
                storage.setProgressMessage(String.format("reload[%03d]: HARD TIME! read properties of each Labels(%d).. 57",txSeq, countOfValue)+"%");
                countWithProperties = loadLabelProperties( labels );
*/
                // labelTypes 전체의 dirty 체크
                totalDirty = totalDirty | checkLabelsDirty( labels );
                // labelTypes 업데이트
                storage.setLabelTypes( labels );

                // metaGraph 갱신
                storage.setProgressMessage(String.format("reload[%03d]: Almost done. read relations and make MetaGraph(%d).. 78", txSeq, countOfValue)+"%");
                countOfRelation = loadMetaGraph(storage.getMetaGraph(), labels);
            }
            else totalDirty = true;

            // meta 전체의 dirty 갱신
            storage.setIsDirty( totalDirty );

        }catch( SQLException e ){
            System.out.println("ERROR: " + e.getMessage()+"\n");
            storage.setIsDirty( true );
        }

        // count nodes and edges
        int nodes_size = storage.getNodesSize();
        int edges_size = storage.getEdgesSize();
        storage.setProgressMessage(String.format("reload[%03d]: Complete! Thank you anyway(%d/%d).. 99", txSeq, nodes_size, edges_size)+"%");

        // copy to COPY storage
        // ** 원본 storage의 isDirty가 true이면 storageCopy를 하지 않는다 (false 옵션)
        boolean copied = storage.storageCopy(false);
        if( copied ){
            storage.setIsDirtyCopy( false );
            storage.setTxSeq( txSeq );      // setTxSeq가 호출되면 어쨌든 -1 (초기)상태는 면한다
        }

        // for INFO
        System.out.println(String.format("reload[%03d]: %s,labels=%d(%d/%d),relations=%d,isDirty=%b ==> %s"
                , txSeq, storage.getGraphType().getName(), countOfValue, nodes_size, edges_size
                , countOfRelation, totalDirty, copied ? "OK!" : "pass"));
    }

/*
    // 특정 label에 대한 create, drop 명령어 결과를 meta에 업데이트
    public LabelType updateLabel(RequestDto req){

        // meta 전체의 dirty 갱신
        storage.setIsDirty( false );

        // label 찾는 조건
        LabelType.ElemType type = LabelType.ElemType.NODE;
        if( req.getCommand().equals("elabel") ) type = LabelType.ElemType.EDGE;
        String name = req.getTarget();

        // DB로부터 특정 라벨 읽어오기
        LabelType label = dao.getLabelType(type, name);

        // if not exists, label is null
        if( label != null ) {   // if not null, CREATE or UPDATE
            storage.addLabelType(label);
        }

        return label;
    }
*/
    // 특정 label에 대한 create, drop 명령어 결과를 meta에 업데이트
    public LabelType updateLabelCopy(RequestDto req, LabelType oldLabel){

        // meta 전체의 dirty 갱신
        storage.setIsDirtyCopy( false );

        // label 찾는 조건
        String type = req.getCommand().equals("elabel") ? "edges" : "nodes";
        String name = !req.getType().equals(RequestDto.RequestType.RENAME.toString())
                      ? req.getTarget() : req.getOptions();  // oldName : newName

        // DB로부터 특정 라벨 읽어오기
        LabelType newLabel = dao.getLabelType(type, name);
        if( newLabel != null ) {   // if not null, CREATE or UPDATE
            if( req.getType().equals(RequestDto.RequestType.CREATE.toString()) ){
                storage.addLabelTypeCopy(newLabel);          // add New
                newLabel.setIsDirty(true);
            }
            else if( req.getType().equals(RequestDto.RequestType.RENAME.toString()) ){
                if( oldLabel != null ){
                    oldLabel.setName( newLabel.getName() );  // update Name
                    oldLabel.setIsDirty(true);
                    storage.updateLabelTypeCopy(oldLabel, req.getType());
                    newLabel = oldLabel;
                }
            }
            else if( req.getType().equals(RequestDto.RequestType.COMMENT.toString()) ){
                if( oldLabel != null ){
                    oldLabel.setDesc( newLabel.getDesc() );  // update Desc
                    oldLabel.setIsDirty(true);
                    storage.updateLabelTypeCopy(oldLabel, req.getType());
                    newLabel = oldLabel;
                }
            }
        }
        else{
            if( req.getType().equals(RequestDto.RequestType.DROP.toString()) ){
                if( oldLabel != null ){
                    storage.removeLabelTypeCopy(oldLabel);   // remove Old
                    oldLabel.setIsDirty(true);
                }
            }
        }

        return newLabel != null ? newLabel : oldLabel;
    }

    public List<PgprocType> getPgprocList(){
        return dao.getPgprocList();
    }

    public PgprocType getPgprocDetail(String id){
        return dao.getPgprocDetail(id);
    }

    public List<String> getPglangList(){
        return dao.getPglangList();
    }

    public ResponseDto savePgproc(PgprocType proc){
        String sql = String.format(
                "CREATE OR REPLACE FUNCTION %s " +
                "( %s ) RETURNS %s AS $$ \n%s\n $$ LANGUAGE %s;",
                proc.getName(), proc.getArgs_type(), proc.getRtn_type(),
                proc.getSource(), proc.getLang() );
        String sqlType = String.format("UPDATE function '%s'", proc.getName());
        ResponseDto result = dao.doSchemaUpdate(sqlType, sql);

        if( result.getState().equals(ResponseDto.StateType.SUCCESS) ){
            sql = String.format("COMMENT ON FUNCTION %s ( %s ) IS '%s';",
                    proc.getName(), proc.getArgs_type(), proc.getDesc() );
            dao.doSchemaUpdate("", sql);
        }

        return result;
    }

    public ResponseDto deletePgproc(PgprocType proc){
        String sql = String.format(
                "DROP FUNCTION IF EXISTS %s ( %s );",
                proc.getName(), proc.getArgs_type() );
        String sqlType = String.format("DROP function '%s'", proc.getName());
        ResponseDto result = dao.doSchemaUpdate(sqlType, sql);

        return result;
    }

}