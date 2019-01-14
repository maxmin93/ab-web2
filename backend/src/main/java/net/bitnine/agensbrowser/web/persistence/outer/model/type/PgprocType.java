package net.bitnine.agensbrowser.web.persistence.outer.model.type;

import org.json.simple.JSONObject;

public class PgprocType implements Cloneable {

    String id = "";
    String name = "";
    String args_type = "";
    String rtn_type = "";
    String desc = "";
    String type = "normal";
    String lang = "";
    String source = "";

    public PgprocType() {}
    public PgprocType(String name, String args_type, String rtn_type) {
        this.name = name;
        this.args_type = args_type;
        this.rtn_type = rtn_type;
    }
    public PgprocType(String id, String name, String args_type, String rtn_type, String lang) {
        this(name, args_type, rtn_type);
        this.id = id;
        this.lang = lang;
    }
    public PgprocType(String id, String name, String args_type, String rtn_type, String lang,
                      String type, String desc, String source) {
        this(id, name, args_type, rtn_type, lang);
        this.type = type;
        this.desc = desc;
        this.source = source;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getDesc() { return desc; }
    public void setDesc(String desc) { this.desc = desc; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getArgs_type() { return args_type; }
    public void setArgs_type(String args_type) { this.args_type = args_type; }

    public String getRtn_type() { return rtn_type; }
    public void setRtn_type(String rtn_type) { this.rtn_type = rtn_type; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getLang() { return lang; }
    public void setLang(String lang) { this.lang = lang; }

    @Override
    public String toString() {
        return "PgprocType{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", lang='" + lang + '\'' +
                '}';
    }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", "pgproc");

        json.put("id", id);
        json.put("name", name);
        json.put("type", type);
        json.put("desc", desc);
        json.put("args_type", args_type);
        json.put("rtn_type", rtn_type);
        json.put("lang", lang);
        json.put("source", source);
        return json;
    }

    @Override
    public int hashCode() {
        final int[] prime = { 17, 29, 31, 37, 73 };
        int result = prime[0] * ((id == null) ? 0 : id.hashCode());
        result += prime[1] * ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final PgprocType other = (PgprocType) obj;
        // oid가 같을 경우
        if (this.id != null && other.id != null && this.id.equals(other.id) ) return true;
        // name과 type이 같을 경우 (id가 없는 어쩔수 없는 경우를 감안해)
        if (this.name != null && other.name != null && this.name.equals(other.name) )
            if (this.type != null && other.type != null && this.type.equals(other.type) )
                return true;
        return false;
    }
}
