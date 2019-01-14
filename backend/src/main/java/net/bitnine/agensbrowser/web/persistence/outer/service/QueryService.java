package net.bitnine.agensbrowser.web.persistence.outer.service;

import net.bitnine.agensbrowser.web.message.*;
import net.bitnine.agensbrowser.web.persistence.inner.dao.AgensLogDao;
import net.bitnine.agensbrowser.web.persistence.inner.model.AgensLog;
import net.bitnine.agensbrowser.web.persistence.outer.dao.QueryDao;
import net.bitnine.agensbrowser.web.persistence.outer.model.EdgeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.ElementType;
import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
import net.bitnine.agensbrowser.web.persistence.outer.model.NodeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;
import net.bitnine.agensbrowser.web.storage.ClientStorage;
import net.bitnine.agensbrowser.web.storage.SchemaStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
@Transactional
public class QueryService {

    private QueryDao queryDao;
    private AgensLogDao logDao;
    private ClientStorage clients;
    private SchemaStorage metaStorage;
    private SchemaService schemaService;
    private GraphService graphService;

    @Autowired
    public QueryService(
        QueryDao queryDao,
        AgensLogDao logDao,
        ClientStorage clients,
        SchemaStorage metaStorage,
        SchemaService schemaService,
        GraphService graphService
    ){
        super();
        this.queryDao = queryDao;
        this.logDao = logDao;
        this.clients = clients;
        this.metaStorage = metaStorage;
        this.schemaService = schemaService;
        this.graphService = graphService;
    }

    public String getNow() { return queryDao.getNow(); }

    ////////////////////////////////////////////////////
    //  gid 관련 graph 유지 전략
    //
    //  case1) 신규 쿼리 (gid=-1)
    //  1. TinkerGraph.open(),
    //  2. gid를 graphStorage에 등록
    //  3. clientStorage에 gid를 갱신
    //  4. 결과를 TinkerGraph에 복사하고 client에 전송
    //
    //  case2) 추가 쿼리 (gid > 0)
    //  1. gid로 graphStorage에서 TinkerGraph 불러오기
    //  2. 결과를 TinkerGraph에 추가하고 client에 전송
    //
    //  ** 조건
    //  1) client는 하나의 gid만 유지 가능
    //  2) gid 갱신의 주체는 client request (gid=-1 로 요청하면 신규 쿼리)
    //  3) 모든 client request 는 jdbc.query 로 수행 (jdbc.update 가 아니라)
    //  4) sqlType 은 무의미하지만 일단은 유지 (결과 있음/없음 정도로만)
    //
    ////////////////////////////////////////////////////

    public ResultDto doQuery(RequestDto req){

        // Log insert
        ClientDto client = clients.getClient(req.getSsid());
        AgensLog log = new AgensLog(client.getUserName(), client.getUserIp(), req.getSql()
                , ResponseDto.StateType.PENDING.toString());
        if( !req.getOptions().equals("loggingOff") ) log = logDao.save(log);

        // run Query
        ResultDto res = null;
        String sqlType = parseSqlType(req.getSql());

        // return RecordType & GraphType : select, match
        if( !sqlType.equals("FORBIDDEN") ){
            // SQL 결과는 result.record 에 저장
            //  ==> record = columns + rows
            // 그중 node, edge 등의 Graph 결과는 result.graph 에 저장
            //  ==> graph = labels + nodes + edges
            res = queryDao.doQuery(req);

            // metaStorage 로부터 labels 를 만들어서 graph에 전달
            // record.graph ==> nodes, edges ==> labels ==> record.graph
            res.getGraph().setLabels( updateMeta(res.getGraph()) );

            // 더불어 내장 tinkerGraph 에 데이터 저장해서 graph-id 를 전달해야
            // 1) copy Graph, 2) graph-id 전달, 3) labels 생성하여 전달, 4) 갱신시 graph-id 삭제

            int nodesSize = res.getGraph().getNodes().size();
            int edgesSize = res.getGraph().getEdges().size();
            if( nodesSize == 0 ) res.setMessage(res.getMessage() + ", no graph");
            else {
                res.setMessage(res.getMessage() + String.format(", graph(nodes=%d, edges=%d)", nodesSize, edgesSize));
                // save GraphType to TinkerGraph
                res.setGid( graphService.saveGraph(req.getGid(), res.getGraph()) );
            }
        }
        else{         // FORBIDDEN: like 'set graph_path'..
            res = new ResultDto(req);
            res.setState(ResponseDto.StateType.FAIL);
            res.setMessage("'"+sqlType+"' is forbidden: "+req.getSql());
        }

        // Log update
        if( !req.getOptions().equals("loggingOff") && log != null && log.getId() != null ){
            log.setState(res.getState().toString());
            log.setMessage(res.getMessage());
            logDao.saveAndFlush(log);
        }

        return res;
    }

    // label_name으로 metaStorage에서 검색한 후 labelType들을 카운팅해서 meta에 부어넣기
    // ** NOTE: 쿼리 결과에 구별가능한 정보는 label_name 밖에 없기 때문임 (oid 출력 없음)
    // ** HINT: AgensGraph에서 node, edge의 id는 "?.??" 으로 생성되는데, 앞자리 이용하면 labelType 검색 가능

    private Set<LabelType> updateMeta(GraphType graph){
        Map<String,LabelType> map = new HashMap<String,LabelType>();
        if( graph.getNodes() != null && graph.getNodes().size() > 0){
            for(Iterator<NodeType> iter = graph.getNodes().iterator(); iter.hasNext(); ){
                NodeType node = iter.next();
                this.upsertLabel(map, (ElementType)node);
            }
        }
        if( graph.getEdges() != null && graph.getEdges().size() > 0){
            for(Iterator<EdgeType> iter = graph.getEdges().iterator(); iter.hasNext(); ){
                EdgeType edge = iter.next();
                this.upsertLabel(map, (ElementType)edge);
            }
        }
        // insert LabelTypes into this.meta
        return new HashSet<LabelType>( map.values() );
    }

    // **NOTE: 로직이 엉성하다. 개선 필요!!
    private void upsertLabel(Map<String,LabelType> map, ElementType ele){
        LabelType label = null;
        // 있는 경우 : update
        if( map.containsKey(ele.getLabel()) ){
            label = map.get(ele.getLabel());
        }
        //
        // **NOTE : Schema 에서 Label의 Properties 를 저장하지 않기로 했기 때문에
        //      ==> 쿼리 결과인 여기서 PropertyType 을 생성해야 함
        //
        // 없는 경우 : insert
        else{
            // ** NOTE: 밖으로 나가는 데이터이기 때문에 storageCopy 에서 찾아 작성
            LabelType temp = metaStorage.findLabelTypeCopy(ele.getGroup(), ele.getLabel());
            if( temp != null ){
                try{
                    label = (LabelType) temp.clone();
                    label.setSize(0L);
                }catch( CloneNotSupportedException ce ){
                    System.out.println("upsertLabel('"+ele.getLabel()+"'): CloneNotSupportedException->"+ce.getMessage()+"\n");
                    label = ele.getLabelType();     // ele의 내용으로부터 labelType 추출
                }
            }
            // metaStorage에도 없는 경우는? (ex: 방금 생성된 labelType) : 임시 ID로 create
            else {
                label = ele.getLabelType();         // ele의 내용으로부터 labelType 추출
            }
            map.put(ele.getLabel(), label);
        }
        // **NOTE: 같은 key 이면 size 증가, 새로운 key 이면 추가
        label.incrementSizeProperties(ele.getProps());
        label.incrementSize();
    }

    // ** NOTE: regex 패턴으로 처리하기엔 노력대비 효과가 적음. 시간도 없고.
    //          (나중에 cypher 파서를 구해서 문법오류까지 잡아주는 것으로)
    // ==> 패턴매칭으로 어떤 label 에 어떤 변경이 생기는지 알면 meta를 즉시 수정하는 것으로 개선 가능!
    public static final String parseSqlType(String sql){
        String query = new String(sql).toLowerCase();

        if( query.startsWith("match ") || query.startsWith("merge ") ){
            if( query.indexOf("return ")>3 ) return "SELECT";
            if( query.indexOf("delete ")>3 || query.indexOf("delete(")>3 ) return "DELETE";
            if( query.indexOf("set ")>3 ) return "UPDATE";
            if( query.indexOf("merge ")>3 ) return "MERGE";
        }
        else if( query.startsWith("select ") ) return "SELECT";
        else if( query.startsWith("create ") ) return "CREATE";
        else if( query.startsWith("drop ") ) return "DROP";
        else if( query.startsWith("delete ") ) return "DELETE";
        else if( query.startsWith("update ") ) return "UPDATE";
        else if( query.startsWith("alter ") ) return "ALTER";
        else if( query.startsWith("comment ") ) return "COMMENT";
        // 금지어
        else if( query.startsWith("set graph_path") || query.startsWith("set search_path") ) return "FORBIDDEN";

        return sql.substring(0,12).toUpperCase()+"..";
    }

    public LabelDto doCommandSync(RequestDto req){

        // ** NOTE: storageCopy에서 가져온다
        // if not exists, dto.label is null
        LabelDto prevDto = metaStorage.getLabelDtoCopy( req );

        // Log insert
        ClientDto client = clients.getClient(req.getSsid());
        AgensLog log = new AgensLog(client.getUserName(), client.getUserIp(), req.getSql()
                , ResponseDto.StateType.PENDING.toString());
        log = logDao.save(log);

        // run Query
        String sqlType = parseSqlType(req.getSql());
        if( sqlType.equals(req.getType()) ) sqlType += "::"+req.getCommand();
        else sqlType += "::"+req.getType();
        ResultDto resQuery = queryDao.doUpdate(req, sqlType);
        String sql = req.getSql();
        String message = resQuery.getMessage();

        // 앞에 doUpdate() 에서 실패했다면 여기서 중단
        if( resQuery.getState().equals(ResponseDto.StateType.FAIL) ){
            LabelDto result = new LabelDto(req);
            result.setState( resQuery.getState() );
            result.setMessage( message+"\n To update Schema is aborted" );
            // Log update
            if( log != null && log.getId() != null ){
                log.setState(resQuery.getState().toString());
                log.setMessage(resQuery.getMessage());
                logDao.saveAndFlush(log);
            }
            return result;
        }

        /////////////////////////////////////////////

        // LabelDto에 실행 결과를 넣어서 반환
        LabelDto result = new LabelDto(req);
        result.setState( resQuery.getState() );
        result.setMessage( message );

        //
        // ** NOTE: doCommand()에 의한 갱신 정보는 storageCopy에 적용된다
        //
        // update SchemaStorage from DB: if not exists, label is null
        LabelType currLabel = schemaService.updateLabelCopy(req, prevDto.getLabel());
        result.setLabel( currLabel );
        result.setMessage( message + "\n And schema storage is updated");

        // Log update
        if( log != null && log.getId() != null ){
            log.setQuery(sql);
            log.setState(result.getState().toString());
            log.setMessage(result.getMessage());
            logDao.saveAndFlush(log);
        }

        return result;
    }

    ///////////////////////////////////////////////////////
    //  ** NOTE: doQuerySync 대신에 doQuery 사용할 것임 (2018-09-19)
    //  ==> 일단은 코드 살려두기
    //
    public ResultDto doQuerySync(RequestDto req){

        // Log insert
        ClientDto client = clients.getClient(req.getSsid());
        AgensLog log = new AgensLog(client.getUserName(), client.getUserIp(), req.getSql()
                , ResponseDto.StateType.PENDING.toString());
        if( !req.getOptions().equals("loggingOff") ) log = logDao.save(log);

        // run Query
        ResultDto res = null;
        String sqlType = parseSqlType(req.getSql());

        // return RecordType & GraphType : select, match
        if( sqlType.equals("SELECT") ){
            res = queryDao.doQuery(req);
            // metaStorage 로부터 labels 를 만들어서 graph에 전달
            // record.graph ==> nodes, edges ==> labels ==> record.graph
            res.getGraph().setLabels( updateMeta(res.getGraph()) );

            // 더불어 내장 tinkerGraph 에 데이터 저장해서 graph-id 를 전달해야
            // 1) copy Graph, 2) graph-id 전달, 3) labels 생성하여 전달, 4) 갱신시 graph-id 삭제

            int nodesSize = res.getGraph().getNodes().size();
            int edgesSize = res.getGraph().getEdges().size();
            if( nodesSize == 0 ) res.setMessage(res.getMessage() + ", no graph");
            else {
                res.setMessage(res.getMessage() + String.format(", graph(nodes=%d, edges=%d)", nodesSize, edgesSize));
                // save GraphType to TinkerGraph
                res.setGid( graphService.saveGraph( -1L, res.getGraph()) );
            }
        }
        else if( sqlType.equals("FORBIDDEN") ){         // set graph_path
            res = new ResultDto(req);
            res.setState(ResponseDto.StateType.FAIL);
            res.setMessage("'SET graph_path' is forbidden. This product cannot change graph_path");
        }
        // no return : create, insert, update, delete, merge, set ..
        else{
            res = queryDao.doUpdate(req, sqlType);
        }

        // Log update
        if( !req.getOptions().equals("loggingOff") && log != null && log.getId() != null ){
            log.setState(res.getState().toString());
            log.setMessage(res.getMessage());
            logDao.saveAndFlush(log);
        }

        return res;
    }

}