package net.bitnine.agensbrowser.web.persistence.outer.model;

import net.bitnine.agensbrowser.web.persistence.outer.model.type.ColumnType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecordType implements Serializable {

    private static final long serialVersionUID = -5575955989067231552L;

    private List<ColumnType> cols;
    private List<List<Object>> rows;

    public RecordType(){
        this.cols = new ArrayList<ColumnType>();
        this.rows = new ArrayList<List<Object>>();
    }

    public RecordType(List<ColumnType> meta, List<List<Object>> rows){
        this.cols = meta;
        this.rows = rows;
    }

    public List<ColumnType> getCols() { return cols; }
    public List<List<Object>> getRows() { return rows; }

    public void setCols(List<ColumnType> cols) { this.cols = cols; }
    public void setRows(List<List<Object>> rows) { this.rows = rows; }

    @Override
    public String toString() {
        return "{\"record\": "+ toJson().toJSONString() + "}";
    }

    public JSONObject toJson(){
        // group: graph
        JSONObject json = new JSONObject();
        json.put("group", "record");
        json.put("cols_size", cols.size());
        json.put("rows_size", rows.size());
        return json;
    }

    public List<Object> toJsonList(){
        List<Object> jsonList = new ArrayList<Object>();
        jsonList.add( (Object)this.toJson() );

        // group: columns
        for(Iterator<ColumnType> iter = cols.iterator(); iter.hasNext();){
            jsonList.add(iter.next().toJson());
        }

        // group: rows
        Long idx = 0L;
        for(Iterator<List<Object>> row = rows.iterator(); row.hasNext(); idx+=1L){
            JSONObject rowObject = new JSONObject();
            rowObject.put("group", "rows");
            rowObject.put("idx", idx);

            JSONArray cellArray = new JSONArray();
            for(Iterator<Object> col = row.next().iterator(); col.hasNext();){
                cellArray.add(col.next());
            }
            rowObject.put("row", cellArray);

            jsonList.add(rowObject);
        }

        return jsonList;
    }

}
