package net.bitnine.agensbrowser.web.message;

import org.json.simple.JSONObject;

import java.io.Serializable;

public class RequestDto implements Serializable {

    private static final long serialVersionUID = -8735609790292961085L;

    private String ssid;    // Session ID (Token)
    private Long gid;    // Request ID (ab#<number>)

    // ** RequestType description:
    // META : ddl commands like create label, delete label so on..
    // QUERY : dml commands like select/where, match/return, insert/update/delete so on..
    // KILL : user signal of canceling task (like long-time query)

    public static enum RequestType { CREATE, DROP, QUERY, KILL, NONE, RENAME, COMMENT }
    private RequestType type;

    private String sql;
    private String command;
    private String target;
    private String options;

    public RequestDto(){
        this.ssid = "";
        this.gid = -1L;
        this.type = RequestType.NONE;
        this.sql = "";
        this.command = "";
        this.target = "";
        this.options = "{}";
    }
    public RequestDto(String ssid, Long gid) {
        this.ssid = ssid;
        this.gid = gid;
        this.type = RequestType.NONE;
        this.sql = "";
        this.command = "";
        this.target = "";
        this.options = "{}";
    }

    static public RequestType toRequestType(String reqType){
        if( reqType.equalsIgnoreCase(RequestType.CREATE.toString()) ) return RequestType.CREATE;
        else if( reqType.equalsIgnoreCase(RequestType.DROP.toString()) ) return RequestType.DROP;
        else if( reqType.equalsIgnoreCase(RequestType.QUERY.toString()) ) return RequestType.QUERY;
        else if( reqType.equalsIgnoreCase(RequestType.KILL.toString()) ) return RequestType.KILL;
        else if( reqType.equalsIgnoreCase(RequestType.RENAME.toString()) ) return RequestType.RENAME;
        else if( reqType.equalsIgnoreCase(RequestType.COMMENT.toString()) ) return RequestType.COMMENT;
        else return RequestType.NONE;
    }

    public String getSsid() { return ssid; }
    public Long getGid() { return gid; }
    public String getType() { return type.toString(); }
    public String getSql() { return sql; }
    public String getCommand() { return command; }
    public String getTarget() { return target; }
    public String getOptions() { return options; }

    public void setSsid(String ssid) { this.ssid = ssid; }
    public void setGid(Long gid) { this.gid = gid; }
    public void setType(RequestType type) { this.type = type; }
    public void setType(String type) { this.type = toRequestType(type); }
    public void setSql(String sql) { this.sql = sql; }
    public void setCommand(String command) { this.command = command; }
    public void setTarget(String target) { this.target = target; }
    public void setOptions(String options) { this.options = options; }

    @Override
    public String toString() {
        return "{\"request\": " + toJson().toJSONString() + "}";
    }
    
    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("ssid", ssid);
        json.put("gid", gid);
        json.put("type", type.toString());
        json.put("sql", sql);
        json.put("command", command);
        json.put("target", target);
        json.put("options", options);
        return json;
    }
}
