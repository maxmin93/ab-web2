package net.bitnine.agensbrowser.web.persistence.outer.dao;

import net.bitnine.agensbrowser.web.message.RequestDto;
import net.bitnine.agensbrowser.web.message.ResultDto;
import net.bitnine.agensbrowser.web.persistence.outer.model.EdgeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
import net.bitnine.agensbrowser.web.persistence.outer.model.NodeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.ColumnType;
import net.bitnine.agensbrowser.web.util.Jsonb;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

@Repository
public class QueryDao {

    @Autowired
    @Qualifier("outerJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Value("${agens.outer.datasource.graph_path}")
    private String graphPath;

    @Value("${agens.outer.datasource.max-rows}")
    private int maxRows;
    @Value("${agens.api.query-timeout}")
    private int queryTimeout;

    public QueryDao() {
        super();
    }

    public String getGraphPath() { return graphPath; }

    // Long-time query : for TEST
    public String getNow(){

        String query = "select current_timestamp";
        Timestamp now = jdbcTemplate.queryForObject(query, Timestamp.class);
        String dateString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(now);

        try{
            Thread.sleep(queryTimeout);
        }catch (InterruptedException e){
            System.out.println(e.getMessage());
        }

        return dateString;
    }

    // Query Results
    // ==>
    // 1) GraphType Types: GraphType, Edges, Nodes
    // 2) Primitive Types: Number, String, Array, Object, Boolean, NULL
    // 3) GraphType Types + Primitive Types
    // 4) DDL return : (success or fail) + message

    public ResultDto doUpdate(RequestDto req, String sqlType) {

        // for DEBUG
        System.out.println("doUpdate: " + req.getSql());

        ResultDto result = new ResultDto(req);
        String message = "";

        // execute query and get affected rows
        // ** NOTE: affected rows count is not returned
        // <== PreparedStatement 사용도 해 보았지만 소용없었음 (jdbctemplate.update와 동일)
        try {
            jdbcTemplate.setQueryTimeout(queryTimeout);
            int rows = jdbcTemplate.update(req.getSql());
            // message = "'"+ sqlType + "' affected "+rows+" rows\n";   // rows 반환이 안되어 제외!
            System.out.println(String.format("'%s' affected (%d rows)", sqlType, rows));
            message = String.format("'%s' affected", sqlType);
            result.setMessage(message);
            result.setState(ResultDto.StateType.SUCCESS);
            result.setRequest(req);
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
        System.out.println("==> " + message);
        return result;
    }

    public ResultDto doQuery(RequestDto req) {

        // for DEBUG
        System.out.println("doQuery: " + req.getSql());

        ResultDto result = new ResultDto(req);
        String message = "";

        // execute query and get records
        try {
            jdbcTemplate.setMaxRows(maxRows);
            jdbcTemplate.setQueryTimeout(queryTimeout);
            result = jdbcTemplate.query( req.getSql(), new ResultExtractor() );

            message = "return "+result.getRecord().getRows().size()
                    +" rows (cols="+result.getRecord().getCols().size()+")";
            result.setMessage(message);
            result.setState(ResultDto.StateType.SUCCESS);
            result.setRequest(req);
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
        System.out.println("==> " + message);

        return result;
    }

    private static final class ResultExtractor implements ResultSetExtractor<ResultDto> {

        @Override
        public ResultDto extractData(ResultSet rs) throws SQLException {
            ResultDto result = new ResultDto();

            // MetaData 추출
            ResultSetMetaData rsmd = rs.getMetaData();
            List<ColumnType> meta = result.getRecord().getCols();
            int columnCount = rsmd.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                String colLabel = rsmd.getColumnLabel(i+1).toLowerCase();
                String colType = rsmd.getColumnTypeName(i+1).toLowerCase();
                ColumnType column = new ColumnType(colLabel, convertTypeName(colType), i);
                meta.add( column );
                // for DEBUG
                System.out.println(column.toString() + " <== " + colType);
            }

            List<List<Object>> rows = result.getRecord().getRows();
            while( rs.next() ){
                List<Object> row = new ArrayList<Object>();
                for (int i = 0; i < columnCount; i++) {
                    Object value = convertValue(result.getGraph(), meta.get(i).getType(), rs.getObject(i+1));
                    row.add(value);
                }
                rows.add(row);
            }

            return result;
        }

        // class ColumnType:
        // enum ValueType { NODE, EDGE, GRAPH, NUMBER, STRING, ID, ARRAY, OBJECT, BOOLEAN, NULL };
        private static final ColumnType.ValueType convertTypeName(String colType){
            final String[] numberTypes = { "numeric", "decimal", "int", "int2", "int4", "int8", "long", "serial2", "serial4", "serial8", "float", "float4", "float8", "double" };
            final String[] stringTypes = { "string", "text", "varchar", "char" };
            final String[] idTypes = { "graphid" };
            final String[] nodeTypes = { "vertex", "node" };
            final String[] edgeTypes = { "edge" };
            final String[] graphTypes = { "graphpath", "graph" };
            final String[] objectTypes = { "jsonb", "json", "object", JSONObject.class.getTypeName() };
            final String[] arrayTypes = { "array", JSONArray.class.getTypeName() };
            final String[] booleanTypes = { "boolean", "bool" };

            if( Arrays.asList(nodeTypes).contains(colType) ) return ColumnType.ValueType.NODE;
            else if( Arrays.asList(edgeTypes).contains(colType) ) return ColumnType.ValueType.EDGE;
            else if( Arrays.asList(graphTypes).contains(colType) ) return ColumnType.ValueType.GRAPH;
            else if( Arrays.asList(numberTypes).contains(colType) ) return ColumnType.ValueType.NUMBER;
            else if( Arrays.asList(stringTypes).contains(colType) ) return ColumnType.ValueType.STRING;
            else if( Arrays.asList(idTypes).contains(colType) ) return ColumnType.ValueType.ID;
            else if( Arrays.asList(objectTypes).contains(colType) ) return ColumnType.ValueType.OBJECT;
            else if( Arrays.asList(arrayTypes).contains(colType) ) return ColumnType.ValueType.ARRAY;
            else if( Arrays.asList(booleanTypes).contains(colType) ) return ColumnType.ValueType.BOOLEAN;

            return ColumnType.ValueType.NULL;
        }

        // class ColumnType:
        // enum ValueType { NODE, EDGE, GRAPH, NUMBER, STRING, ARRAY, OBJECT, BOOLEAN, NULL };
        private static final Object convertValue(GraphType result, ColumnType.ValueType colType, Object value) {

            if( colType.equals(ColumnType.ValueType.NODE) ){
                NodeType node = new NodeType();
                try {
                    node.setValue(value.toString());
                    result.getNodes().add(node);
                }catch( SQLException e ){
                    System.out.println("convertValue<"+colType.toString()+"> fail: "+e.getCause());
                    System.out.println(value.toString()+"\n");
                }
                return (Object) node.toJson();
            }
            else if( colType.equals(ColumnType.ValueType.EDGE) ){
                EdgeType edge = new EdgeType();
                try {
                    edge.setValue(value.toString());
                    result.getEdges().add(edge);
                }catch( SQLException e ){
                    System.out.println("convertValue<"+colType.toString()+"> fail: "+e.getCause());
                    System.out.println(value.toString()+"\n");
                }
                return (Object) edge.toJson();
            }
            else if( colType.equals(ColumnType.ValueType.GRAPH) ){
                GraphType graph = new GraphType();
                try {
                    graph.setValue(value.toString());
                    result.getNodes().addAll(graph.getNodes());
                    result.getEdges().addAll(graph.getEdges());
                }catch( SQLException e ){
                    System.out.println("convertValue<"+colType.toString()+"> fail: "+e.getCause());
                    System.out.println(value.toString()+"\n");
                }
                return (Object) graph.toJson();
            }
            else if( colType.equals(ColumnType.ValueType.ID) ){
                // TYPE<graphid> example => { "type": "graphid", "value": "4.1234" }
                // ** NOTE: toString() 출력하면 value "4.1234"만 나온다. 완전 신기!!
                return value.toString();
            }
            else if( colType.equals(ColumnType.ValueType.BOOLEAN ) || colType.equals(ColumnType.ValueType.NUMBER)
                    || colType.equals(ColumnType.ValueType.STRING) ){
                return value;
            }
            else if( colType.equals(ColumnType.ValueType.ARRAY) || colType.equals(ColumnType.ValueType.OBJECT) ){
                Jsonb props = new Jsonb();
                try {
                    if( value == null ){    // null check
                        System.out.println("convertValue<"+colType.toString()+"> pass --> value is null");
                        return (Object)null;
                    }
                    props.setValue(value.toString());
                }catch( SQLException e ){
                    System.out.println("convertValue<"+colType.toString()+"> fail: "+e.getCause());
                    System.out.println(value.toString()+"\n");
                }

                return (Object) props.getJsonValue();
            }

            // for DEBUG
            System.out.println("convertValue<"+colType.toString()+"> pass --> "+value.toString());
            return value;
        }
    }

}

