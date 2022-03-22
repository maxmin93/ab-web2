package net.bitnine.agensbrowser.web.persistence.outer.dao;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import net.bitnine.agensbrowser.web.message.ResponseDto;
import net.bitnine.agensbrowser.web.message.ResultDto;
import net.bitnine.agensbrowser.web.persistence.outer.model.HealthType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.DatasourceType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;

import net.bitnine.agensbrowser.web.persistence.outer.model.type.PgprocType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.PropertyType;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

@Repository
public class SchemaDao {

    @Autowired
    @Qualifier("outerJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Value("${agens.outer.datasource.url}")
    private String graphUrl;
    @Value("${agens.outer.datasource.graph_path}")
    private String graphPath;
    @Value("${agens.outer.datasource.username}")
    private String graphOwner;
    @Value("${agens.api.query-timeout}")
    private int queryTimeout;

    public SchemaDao() {
        super();
    }

    public String getGraphPath() { return graphPath; }

    // if jsonb, AgensGraph is over 1.3 (and if numeric, under 1.2)
    public float checkAGVersion(){
        String query = "return pg_typeof(3.1)";
        String result = jdbcTemplate.queryForObject(query, String.class);
        if( result.equals("jsonb") ) return 1.3f;
        // else
        return 1.2f;
    }

    // Long-time query : for TEST
    public String getNow(){
        String query = "select current_timestamp";
        Timestamp now = jdbcTemplate.queryForObject(query, Timestamp.class);
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(now);
    }

    public HealthType getHealthTest(){
        HikariDataSource source = (HikariDataSource) jdbcTemplate.getDataSource();
        HealthType healthInfo = new HealthType();

        HikariPool hikariPool = (HikariPool) new DirectFieldAccessor(source).getPropertyValue("pool");
        healthInfo.setBusyConnections( hikariPool.getActiveConnections() );
        healthInfo.setEstablishedConnections( hikariPool.getTotalConnections() );
        healthInfo.setIdleConnections( hikariPool.getIdleConnections() );

        healthInfo.setJdbcUrl( source.getJdbcUrl() );
        healthInfo.setUsername( source.getUsername() );
        healthInfo.setClosed( source.isClosed() );
        healthInfo.setCpType( "hikari" );
        healthInfo.setTestTime( this.getNow() );
        return healthInfo;
    }

    /*
﻿SELECT g.nspid as gr_oid, g.graphname as gr_name,
    pg_catalog.pg_get_userbyid(nspowner) as gr_owner,
    coalesce(null, pg_catalog.obj_description(g.nspid), '') as gr_desc
FROM pg_catalog.ag_graph g
    LEFT JOIN pg_catalog.pg_namespace n on n.nspname = g.graphname
WHERE g.graphname = 'imdb_graph';
 */
    public DatasourceType getGraphType() {

        String query = "SELECT g.nspid as gr_oid, g.graphname as gr_name,"
                + " pg_catalog.pg_get_userbyid(nspowner) as gr_owner,"
                + " coalesce(null, pg_catalog.obj_description(g.nspid), '') as gr_desc"
                + " FROM pg_catalog.ag_graph g"
                + " LEFT JOIN pg_catalog.pg_namespace n on n.nspname = g.graphname"
                + " WHERE g.graphname = '"+ graphPath +"'";

        List<DatasourceType> results = null;
        try {
            results = jdbcTemplate.query(query,
                    (rs, rowNum) -> new DatasourceType(
                            rs.getString("gr_oid"),
                            rs.getString("gr_name"),
                            rs.getString("gr_owner"),
                            rs.getString("gr_desc"))
            );
            // 정상 결과 리턴
            if( results.size() > 0 ){
                DatasourceType graph = (DatasourceType) results.get(0);
                graph.setJdbcUrl(graphUrl);
                graph.setIsDirty(false);
                return graph;
            }
        }catch( DataAccessException de ){
            System.out.println("getGraph(): DataAccessException->" + de.getMessage()
                    + ", SQL Code->" + ((SQLException)de.getCause()).getSQLState());
        }

        return null;
    }

/*
SELECT l.labid as la_oid, l.labname as la_name,
	case when l.labkind='e' then 'edge' else 'node' end as la_type,
    pg_catalog.pg_get_userbyid(c.relowner) as la_owner,
    coalesce(null, pg_catalog.obj_description(l.oid, 'ag_label'), '') as la_desc,
    pg_size_pretty(pg_total_relation_size( l.labname::varchar )) as la_volm,
    coalesce(null, u.n_live_tup, 0) as la_size
FROM pg_catalog.ag_label l
    INNER JOIN pg_catalog.ag_graph g ON g.oid = l.graphid
    LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid = l.relid
    LEFT OUTER JOIN pg_stat_user_tables u on u.relid = l.relid
WHERE g.graphname = 'imdb_graph' and l.labname not in ('ag_vertex', 'ag_edge')
ORDER BY l.labid;

** TEST: comment 생성문
'comment on '||l.labkind||'label '||l.labname||' is ''oid: '||l.labid::varchar||', name: '||l.labname||', type: '||l.labkind||', owner: agraph'';' as la_desc
 */
    public List<LabelType> getLabelTypes() {
        String query = "SELECT l.labid as la_oid, l.labname as la_name,"
                + " case when l.labkind='e' then 'edges' else 'nodes' end as la_type,"
                + " pg_catalog.pg_get_userbyid(c.relowner) as la_owner,"
                + " coalesce(null, pg_catalog.obj_description(l.oid, 'ag_label'), '') as la_desc,"
                + " pg_size_pretty(pg_total_relation_size( concat( g.graphname::varchar, concat( '.'::varchar, l.labname::varchar )) )) as la_volm,"
                + " coalesce(null, u.n_live_tup, 0) as la_size"
                + " FROM pg_catalog.ag_label l"
                + " INNER JOIN pg_catalog.ag_graph g ON g.oid = l.graphid"
                + " LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid = l.relid"
                + " LEFT OUTER JOIN pg_stat_user_tables u on u.relid = l.relid"
                + " WHERE g.graphname = '"+ graphPath +"' and l.labname not in ('ag_vertex', 'ag_edge')"
                + " ORDER BY l.labid;";

        List<LabelType> results = new ArrayList<LabelType>();
        try {
            results = jdbcTemplate.query(query,
                    (rs, rowNum) -> new LabelType(
                            rs.getString("la_oid"),
                            rs.getString("la_name"),
                            rs.getString("la_type"),
                            rs.getString("la_owner"),
                            rs.getString("la_desc"),
                            rs.getString("la_volm"),
                            rs.getLong("la_size"))
            );
        }catch( DataAccessException de ){
            System.out.println("getLabels(): DataAccessException->" + de.getMessage()
                    + ", SQL Code->" + ((SQLException)de.getCause()).getSQLState());
        }

        return results;
    }

/*
SELECT l.labid as la_oid, l.labname as la_name,
	case when l.labkind='e' then 'edge' else 'node' end as la_type,
    pg_catalog.pg_get_userbyid(c.relowner) as la_owner,
    coalesce(null, pg_catalog.obj_description(l.oid, 'ag_label'), '') as la_desc,
    pg_size_pretty(pg_total_relation_size( l.labname::varchar )) as la_volm,
    coalesce(null, u.n_live_tup, 0) as la_size
FROM pg_catalog.ag_label l
    INNER JOIN pg_catalog.ag_graph g ON g.oid = l.graphid
    LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid = l.relid
    LEFT OUTER JOIN pg_stat_user_tables u on u.relid = l.relid
WHERE g.graphname = 'imdb_graph' and l.labkind = 'v' and l.labname = 'test_label'
limit 1;

// 코멘트 붙이기
// ** 참고 : 스키마, 테이블과 컬럼까지도 되지만 AgensGraph에서는 vlabel, elabel (테이블 레벨)만 가능
﻿comment on vlabel top_user is 'TEST comment';
 */
    public LabelType getLabelType(String type, String labelName) {

        String labKind = "v";
        if( type.equals("edges") ) labKind = "e";

        String query = "SELECT l.labid as la_oid, l.labname as la_name,"
                + " case when l.labkind='e' then 'edges' else 'nodes' end as la_type,"
                + " pg_catalog.pg_get_userbyid(c.relowner) as la_owner,"
                + " coalesce(null, pg_catalog.obj_description(l.oid, 'ag_label'), '') as la_desc,"
                + " pg_size_pretty(pg_total_relation_size( l.labname::varchar )) as la_volm,"
                + " coalesce(null, u.n_live_tup, 0) as la_size"
                + " FROM pg_catalog.ag_label l"
                + " INNER JOIN pg_catalog.ag_graph g ON g.oid = l.graphid"
                + " LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid = l.relid"
                + " LEFT OUTER JOIN pg_stat_user_tables u on u.relid = l.relid"
                + " WHERE g.graphname = '"+ graphPath +"' and l.labkind = '"+ labKind +"' and l.labname = '"+ labelName +"'"
                + " LIMIT 1;";

        List<LabelType> results = new ArrayList<LabelType>();
        try {
            results = jdbcTemplate.query(query,
                    (rs, rowNum) -> new LabelType(
                            rs.getString("la_oid"),
                            rs.getString("la_name"),
                            rs.getString("la_type"),
                            rs.getString("la_owner"),
                            rs.getString("la_desc"),
                            rs.getString("la_volm"),
                            rs.getLong("la_size"))
            );
        }catch( DataAccessException de ){
            System.out.println("getLabel("+labelName+"): DataAccessException->" + de.getMessage()
                    + ", SQL Code->" + ((SQLException)de.getCause()).getSQLState());
        }

        LabelType result = null;
        if (results.size() > 0) result = results.get(0);

        return result;
    }

/*
﻿SELECT count(properties) as tot_cnt
	, ﻿coalesce( sum(case when properties = '{}' then 0 else 1 end), -1) as json_cnt
from imdb_graph.keyword
 */
    public Long getLabelCount(String label) throws SQLException {
        Long count = -1L;
        if( label == null ) return count;

        String query = "SELECT count(id) as tot_cnt from "+graphPath+"."+ label+";";
        try {
            List<Long> result = jdbcTemplate.query(query, (rs, rowNum) -> {
                return rs.getLong(1);       // 전체 카운트
            });
            if (result.size() > 0) count = result.get(0);
        }catch( DataAccessException de ){
            String sqlErrCode = ((SQLException)de.getCause()).getSQLState();
            System.out.println("getLabelCount(): DataAccessException->" + de.getMessage()
                    + ", SQL Code->" + sqlErrCode);
            // ** 08006: connection_failure
            // https://www.postgresql.org/docs/9.4/static/errcodes-appendix.html
            if( sqlErrCode == null || sqlErrCode.equals("08006") ){
                throw new PSQLException("getLabelCount() failed.", PSQLState.CONNECTION_FAILURE, de);
            }
            count = -1L;        // Label status ==> label is dirty
        }

        return count;
    }

/*
﻿SELECT json_data.key as json_key,
	jsonb_typeof(json_data.value) as json_type,
	count(*) as key_count
FROM imdb_graph.keyword, jsonb_each(imdb_graph.keyword.properties) AS json_data
WHERE imdb_graph.keyword.properties <> '{}' and jsonb_typeof(json_data.value) <> 'null'
group by 1, 2
order by 1, 2;

전체 레코드를 읽으면 서버에 부담이 크다. 매뉴얼에 기술하고 최신 1만개만 읽는 것으로 수정
==>
﻿SELECT json_data.key as json_key,
	jsonb_typeof(json_data.value) as json_type,
	count(*) as key_count
FROM ( select properties from imdb_graph.actor_in where
    imdb_graph.actor_in.properties <> '{}' order by id desc limit 10000
	) tmp, jsonb_each(tmp.properties) AS json_data
WHERE jsonb_typeof(json_data.value) <> 'null'
group by 1, 2
order by 1, 2;
 */

    // **NOTE: 전체 스키마를 대상으로는 용도가 없어서 안쓰기로 함 (코드는 남겨두고)
    // ==> 대신 사용자 쿼리에 대해서는 TP3에서 Schema 추출시 Properties를 사용
    public List<PropertyType> getLabelProperties(String label) throws SQLException {
        if( label == null ) return null;

        String query = "SELECT json_data.key as json_key, "+
                "jsonb_typeof(json_data.value) as json_type, "+
                "count(*) as key_count "+
                "FROM ( select properties from "+graphPath+"."+label+" where "+
                graphPath+"."+label+".properties <> '{}' order by id desc limit 10000 "+
	            ") tmp, jsonb_each(tmp.properties) AS json_data "+
                "WHERE jsonb_typeof(json_data.value) <> 'null' "+
                "group by 1, 2 "+
                "order by 1, 2;";

        List<PropertyType> results = null;
        try {
            results = jdbcTemplate.query(query, (rs, rowNum) -> {
                return new PropertyType(
                        rs.getString("json_key"),
                        rs.getString("json_type"),
                        rs.getLong("key_count")
                );
            });
        }catch( DataAccessException de ){
            String sqlErrCode = ((SQLException)de.getCause()).getSQLState();
            System.out.println("getLabelProperties(): DataAccessException->" + de.getMessage()
                    + ", SQL Code->" + sqlErrCode);
            // ** 08006: connection_failure
            // https://www.postgresql.org/docs/9.4/static/errcodes-appendix.html
            if( sqlErrCode == null || sqlErrCode.equals("08006") ){
                throw new PSQLException("getLabelProperties() failed.", PSQLState.CONNECTION_FAILURE, de);
            }
            // 작업을 건너뛰기 위한 결과값 설정
            results = null;
        }

        return results;
    }

/*
select el.labid as e_oid, el.labname as e_name
		, sl.labid as s_oid, sl.labname as s_name, tl.labid as t_oid, tl.labname as t_name
		, tmp.cnt
from (
    match (a)-[r:orders]->(b)
    with graphid_labid(start(r)) as source_id, graphid_labid(id(r)) as edge_id, graphid_labid("end"(r)) as target_id
    return source_id, edge_id, target_id, count(*) as cnt
	) tmp
	INNER JOIN pg_catalog.ag_graph g ON g.graphname = 'northwind_graph'
	INNER JOIN pg_catalog.ag_label el ON el.graphid = g.oid and el.labid = tmp.edge_id
	INNER JOIN pg_catalog.ag_label sl ON sl.graphid = g.oid and sl.labid = tmp.source_id
	INNER JOIN pg_catalog.ag_label tl ON tl.graphid = g.oid and tl.labid = tmp.target_id
order by sl.labid, tl.labid asc;

간단히 구해서 이전 단계에서 읽어들였던 Labels 리스트 하고 연결
==>
match (a)-[r:actor_in]->(b)
with graphid_labid(start(r)) as s_oid, graphid_labid(id(r)) as e_oid, graphid_labid("end"(r)) as t_oid
with e_oid, s_oid, t_oid, count(*) as r_cnt
return e_oid, s_oid, t_oid, r_cnt;
 */
    public List<Long[]> getRelationCount(String label) throws SQLException {
        List<Long[]> result = new ArrayList<Long[]>();
        if( label == null ) return result;

        //
        // **NOTE: 하나의 edge가 여러 유형의 node에 연결될 수 있음
        //         예를 들어, eatable 관계는 포유루인 경우 채소, 고기 모두 가능
        String query = "match (a)-[r:"+label+"]->(b) "+
//
// ** NOTE: AgensGraph v1.3에서 테스트 했더니 in (subquery) 연산이 안먹더라!!
//          => 일단 넘긴 후 edges 등록 전에 source, target 유무 검사 (SchemaService.loadMetaGraph() 에서)
//
//                "where graphid_labid(start(r)) in (select l.labid from pg_catalog.ag_label l INNER JOIN pg_catalog.ag_graph g ON g.oid = l.graphid where g.graphname = '"+graphPath+"' and l.labkind='v' and l.labname <> 'ag_vertex') "+
//                "and graphid_labid(\"end\"(r)) in (select l.labid from pg_catalog.ag_label l INNER JOIN pg_catalog.ag_graph g ON g.oid = l.graphid where g.graphname = '"+graphPath+"' and l.labkind='v' and l.labname <> 'ag_vertex') "+
//
                "with graphid_labid(start(r)) as s_oid, graphid_labid(id(r)) as e_oid, graphid_labid(\"end\"(r)) as t_oid "+
                "with e_oid, s_oid, t_oid, count(*) as r_cnt "+
                "return e_oid, s_oid, t_oid, r_cnt;";
        try {
            result = jdbcTemplate.query(query, (rs, rowNum) -> {
                Long[] cols = new Long[4];
                cols[0] = rs.getLong(1);    // oid of edge node
                cols[1] = rs.getLong(2);    // oid of source node
                cols[2] = rs.getLong(3);    // oid of target node
                cols[3] = rs.getLong(4);    // relation 의 카운트
                return cols;
            });
        }
        catch( DataAccessException de ){
            String sqlErrCode = ((SQLException)de.getCause()).getSQLState();
            System.out.println("getRelationCount(): DataAccessException->" + de.getMessage()
                    + ", SQL Code->" + sqlErrCode);
            // ** 08006: connection_failure
            // https://www.postgresql.org/docs/9.4/static/errcodes-appendix.html
            if( sqlErrCode == null || sqlErrCode.equals("08006") ){
                throw new PSQLException("getRelationCount() failed.", PSQLState.CONNECTION_FAILURE, de);
            }
        }

        return result;
    }

/*
    ** List of pg functions
    ** ==> oid, proname, args_type, rtns_type
    *
SELECT p.oid, p.proname
	, pg_get_function_arguments(p.oid) as args_type
	, pg_get_function_result(p.oid) as rtn_type
	, lg.lanname
FROM pg_proc p
	INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
	INNER JOIN pg_language lg ON (p.prolang = lg.oid)
WHERE ns.nspname = 'sample01_graph'
	and pg_catalog.pg_get_userbyid(p.proowner) = 'agraph'
	and pg_catalog.pg_function_is_visible(p.oid);

    ** Detail of pg function with source code
    ** ==> proname, args_type, rtns_type, prodesc, protype, prolang, prosrc
    *
SELECT p.proname
	, pg_get_function_arguments(p.oid) as args_type
	, pg_get_function_result(p.oid) as rtn_type
	, coalesce(null, pg_catalog.obj_description(p.oid), '') as prodesc
	, CASE
		  WHEN p.proisagg THEN 'agg'
		  WHEN p.proiswindow THEN 'window'
		  WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
		  ELSE 'normal'
	  END as protype
	, lg.lanname as prolang
	, p.prosrc
FROM pg_proc p
	INNER JOIN pg_language lg ON (p.prolang = lg.oid)
WHERE p.oid = 4387342;
 */

    public List<PgprocType> getPgprocList() {
        String query = "SELECT p.oid, p.proname " +
                ", pg_get_function_arguments(p.oid) as args_type " +
                ", pg_get_function_result(p.oid) as rtn_type " +
                ", lg.lanname as prolang " +
                "FROM pg_proc p " +
                "  INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid) " +
                "  INNER JOIN pg_language lg ON (p.prolang = lg.oid) " +
                "WHERE ns.nspname = '" + graphPath + "' " +
                "  and pg_catalog.pg_get_userbyid(p.proowner) = '" + graphOwner + "' " +
                "  and pg_catalog.pg_function_is_visible(p.oid);";

        List<PgprocType> results = new ArrayList<PgprocType>();
        try {
            results = jdbcTemplate.query(query,
                    (rs, rowNum) -> new PgprocType(
                            rs.getString("oid"),
                            rs.getString("proname"),
                            rs.getString("args_type"),
                            rs.getString("rtn_type"),
                            rs.getString("prolang"))
            );
        }catch( DataAccessException de ){
            System.out.println("getPgprocList(): DataAccessException->" + de.getMessage()
                    + ", SQL Code->" + ((SQLException)de.getCause()).getSQLState());
        }

        return results;
    }

    public PgprocType getPgprocDetail(String id) {
        String query = "SELECT p.oid, p.proname " +
                ", pg_get_function_arguments(p.oid) as args_type " +
                ", pg_get_function_result(p.oid) as rtn_type " +
                ", lg.lanname as prolang " +
                ", CASE " +
                "    WHEN p.proisagg THEN 'agg' " +
                "    WHEN p.proiswindow THEN 'window' " +
                "    WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger' " +
                "    ELSE 'normal' " +
                "  END as protype " +
                ", coalesce(null, pg_catalog.obj_description(p.oid), '') as prodesc " +
	            ", p.prosrc " +
                "FROM pg_proc p " +
                "  INNER JOIN pg_language lg ON (p.prolang = lg.oid) " +
                "WHERE p.oid = " + id + ";";

        List<PgprocType> results = null;
        try {
            results = jdbcTemplate.query(query,
                    (rs, rowNum) -> new PgprocType(
                            rs.getString("oid"),
                            rs.getString("proname"),
                            rs.getString("args_type"),
                            rs.getString("rtn_type"),
                            rs.getString("prolang"),
                            rs.getString("protype"),
                            rs.getString("prodesc"),
                            rs.getString("prosrc"))
            );
            // 정상 결과 리턴
            if( results.size() > 0 ){
                PgprocType pgproc = (PgprocType) results.get(0);
                return pgproc;
            }
        }catch( DataAccessException de ){
            System.out.println("getPgprocDetail(): DataAccessException->" + de.getMessage()
                    + ", SQL Code->" + ((SQLException)de.getCause()).getSQLState());
        }

        return null;
    }

/*
select lanname from pg_language;
 */

    public List<String> getPglangList() {
        String query = "select lanname from pg_language;";

        List<String> results = new ArrayList<String>();
        try {
            results = jdbcTemplate.query(query,
                    (rs, rowNum) -> new String(rs.getString("lanname"))
            );
        }catch( DataAccessException de ){
            System.out.println("getPglangList(): DataAccessException->" + de.getMessage()
                    + ", SQL Code->" + ((SQLException)de.getCause()).getSQLState());
        }

        return results;
    }

/*
COMMENT ON FUNCTION pymax(a integer, b integer) IS 'pymax example description 1';
 */
    public ResponseDto doSchemaUpdate(String sqlType, String sql) {

        ResponseDto result = new ResponseDto();
        String message = "";

        // execute query and get affected rows
        // ** NOTE: affected rows count is not returned
        // <== PreparedStatement 사용도 해 보았지만 소용없었음 (jdbctemplate.update와 동일)
        try {
            jdbcTemplate.setQueryTimeout(queryTimeout);
            int rows = jdbcTemplate.update(sql);
            // message = "'"+ sqlType + "' affected "+rows+" rows\n";   // rows 반환이 안되어 제외!
            message = sqlType + " affected";
            result.setMessage(message);
            result.setState(ResultDto.StateType.SUCCESS);
        } catch (BadSqlGrammarException be) {
            message = "BadSqlGrammarException->" + be.getMessage()
                    + ", SQL Code->" + ((SQLException)be.getCause()).getSQLState();
            result.setMessage(message);
            result.setState(ResultDto.StateType.FAIL);
        } catch( QueryTimeoutException te ){
            message = "QueryTimeoutException->" + te.getMessage()
                    + ", SQL Code->" + ((SQLException)te.getCause()).getSQLState()
                    + "\n Timeout "+((int)(queryTimeout/1000))+" seconds is over. If you want more timeout, modify agens-config.yml";
            result.setMessage(message);
            result.setState(ResultDto.StateType.FAIL);
        } catch( DataAccessException de ){
            message = "DataAccessException->" + de.getMessage()
                    + ", SQL Code->" + ((SQLException)de.getCause()).getSQLState();
            result.setMessage(message);
            result.setState(ResultDto.StateType.FAIL);
        }

        // for DEBUG
        if(sqlType.length() > 0) System.out.println("==> " + message);
        return result;
    }
}
