package net.bitnine.agensbrowser.web.message;

import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DoubleListDto extends ResponseDto implements Serializable {

    private static final long serialVersionUID = -3648282421110299383L;

    private String group = "dlist_dto";
    private Long gid = -1L;
    private String sid = null;
    private String eid = null;
    private List<List<Object>> rows = new ArrayList<List<Object>>();

    public DoubleListDto(String group){
        super();
        this.group = group;
    }
    public DoubleListDto(String group, List<List<Object>> rows){
        this(group);
        this.rows = rows;
    }
    public DoubleListDto(String group, List<List<Object>> rows, Long gid){
        this(group, rows);
        this.gid = gid;
    }
    public DoubleListDto(String group, List<List<Object>> rows, Long gid, String sid, String eid){
        this(group, rows, gid);
        this.sid = sid;
        this.eid = eid;
    }

    @Override
    public String toString(){ return "{\"dlist_dto\": "+ toJson().toJSONString() + "}"; }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", group);
        json.put("state", state.toString());
        json.put("message", message);

        json.put("gid", gid);
        if( sid != null ) json.put("sid", sid);
        if( eid != null ) json.put("eid", eid);
        if( rows != null ) json.put("result", rows);
        return json;
    }

}
