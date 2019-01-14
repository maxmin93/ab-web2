package net.bitnine.agensbrowser.web.persistence.outer.model;

import org.json.simple.JSONObject;

public class HealthType {

    private int busyConnections = 0;
    private int establishedConnections = 0;
    private int idleConnections = 0;
    private int activeSessions = 0;
    private String jdbcUrl = "";
    private String userName = "";
    private boolean isClosed = true;
    private String cpType = "";
    private String testTime = "";
    private String schemaImage = "";
    private String productName = "";
    private String productVersion = "";
    private String description = "";

    public HealthType(){}

    public void setBusyConnections(int busyConnections){ this.busyConnections = busyConnections; }
    public void setEstablishedConnections(int establishedConnections){ this.establishedConnections = establishedConnections; }
    public void setIdleConnections(int idleConnections){ this.idleConnections = idleConnections; }
    public void setActiveSessions(int activeSessions){ this.activeSessions = activeSessions; }
    public void setJdbcUrl(String jdbcUrl){ this.jdbcUrl = jdbcUrl; }
    public void setUsername(String userName){ this.userName = userName; }
    public void setClosed(boolean isClosed){ this.isClosed = isClosed; }
    public void setCpType(String cpType){ this.cpType = cpType; }
    public void setTestTime(String testTime){ this.testTime = testTime; }
    public void setSchemaImage(String schemaImage){ this.schemaImage = schemaImage; }
    public void setProductName(String productName) { this.productName = productName; }
    public void setProductVersion(String productVersion) { this.productVersion = productVersion; }
    public void setDescription(String description) { this.description = description; }

    public int getBusyConnections() { return busyConnections; }
    public int getEstablishedConnections() { return establishedConnections; }
    public int getIdleConnections() { return idleConnections; }
    public int getActiveSessions() { return activeSessions; }
    public String getJdbcUrl() { return jdbcUrl; }
    public String getUserName() { return userName; }
    public boolean isClosed() { return isClosed; }
    public String getCpType() { return cpType; }
    public String getTestTime() { return testTime; }
    public String getSchemaImage() { return schemaImage; }
    public String getProductName() { return productName; }
    public String getProductVersion() { return productVersion; }
    public String getDescription() { return description; }

    public static String getGroup(){ return "health"; }

    @Override
    public String toString(){
        return toJson().toJSONString();
    }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", this.getGroup());        // group = "elements"
        json.put("busy_connections", busyConnections);
        json.put("established_connections", establishedConnections);
        json.put("idle_connections", idleConnections);
        json.put("active_sessions", activeSessions);
        json.put("jdbc_url", jdbcUrl);
        json.put("user_name", userName);
        json.put("is_closed", isClosed);
        json.put("cp_type", cpType);
        json.put("test_time", testTime);
        json.put("schema_image", schemaImage);
        json.put("product_name", productName);
        json.put("product_version", productVersion);
        json.put("description", description);
        return json;
    }

}
