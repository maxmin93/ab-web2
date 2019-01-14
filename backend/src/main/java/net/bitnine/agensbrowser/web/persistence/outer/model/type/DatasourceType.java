package net.bitnine.agensbrowser.web.persistence.outer.model.type;

import org.json.simple.JSONObject;

import javax.persistence.Entity;
import java.io.Serializable;

@Entity
public class DatasourceType implements Serializable, Cloneable {

    private static final long serialVersionUID = -2901884864870956373L;

    private String oid;
    private String name;
    private String owner;
    private String desc;
    private String jdbcUrl;

    private float version;
    private Boolean isDirty;        // whether data is changed or not for properties

    public DatasourceType(){
        this.oid = "-1";
        this.name = "";
        this.owner = "";
        this.desc = "";
        this.jdbcUrl = "";
        this.version = 1.3f;
        this.isDirty = true;
    }
    public DatasourceType(String oid, String name, String owner, String desc) {
        this.oid = oid;
        this.name = name;
        this.owner = owner;
        this.desc = desc;
        this.jdbcUrl = "";
        this.version = 1.3f;
        this.isDirty = true;
    }

    public String getOid() { return oid; }
    public String getName() { return name; }
    public String getOwner() { return owner; }
    public String getDesc() { return desc; }
    public String getJdbcUrl() { return jdbcUrl; }
    public float getVersion() { return version; }
    public Boolean getIsDirty() { return isDirty; }

    public void setOid(String oid) { this.oid = oid; }
    public void setName(String name) { this.name = name; }
    public void setOwner(String owner) { this.owner = owner; }
    public void setDesc(String desc) { this.desc = desc; }
    public void setIsDirty(Boolean isDirty) { this.isDirty = isDirty; }
    public void setVersion(float version) { this.version = version; }
    public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }

    @Override
    public String toString() {
        return "{\"datasource\": " + toJson().toJSONString() + "}";
    }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", "datasource");

        json.put("oid", oid);
        json.put("name", name);
        json.put("owner", owner);
        json.put("desc", desc);
        json.put("jdbc_url", jdbcUrl);
        json.put("version", version);
        json.put("is_dirty", isDirty);
        return json;
    }

    @Override
    public int hashCode() {
        final int[] prime = { 17, 29, 31, 37, 73 };
        int result = prime[0];
        result = prime[1] * result + prime[2] * ((oid == null) ? 0 : oid.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final DatasourceType other = (DatasourceType) obj;
        // oid가 같을 경우
        if (this.oid != null && other.oid != null && this.oid.equals(other.oid) ) return true;
        return false;
    }

    public Object clone() throws CloneNotSupportedException {
        return (DatasourceType) super.clone();
    }

}
