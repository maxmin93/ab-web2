package net.bitnine.agensbrowser.web.message;

import org.json.simple.JSONObject;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.UUID;

public class ClientDto extends ResponseDto implements Serializable {

    private String ssid;        // UUID
    private String token;       // JWT
    private Boolean valid;

    private String userName;    // 일단은 dbUser를 userName으로 대신해서 사용
    private String userIp;
    private Long gid = -1L;     // 최근 사용한 gid

    private String productName = "";
    private String productVersion = "";
    private String mode = "prod";
    private Boolean animationEnabled = false;
    private Boolean titleShown = false;
    private String downloadUrl = "/api/file/download/";

    private Timestamp timestamp;

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public final String getConnectDate() { return dateFormat.format(timestamp); }

    public ClientDto(String userName, String userIp) {
        super();
        this.ssid = UUID.randomUUID().toString().replace("-","");
        this.token = null;
        this.valid = false;
        this.userName = userName;
        this.userIp = userIp;
        this.timestamp = new Timestamp(System.currentTimeMillis());;
    }

    public String getSsid() { return ssid; }
    public String getToken() { return token; }
    public Boolean getValid() { return valid; }
    public String getUserName() { return userName; }
    public String getUserIp() { return userIp; }
    public Long getGid() { return gid; }
    public String getDownloadUrl() { return downloadUrl; }
    public Boolean getTitleShown() { return titleShown; }

    public void setTitleShown(Boolean titleShown) { this.titleShown = titleShown; }
    public void setDownloadUrl(String downloadUrl) { this.downloadUrl = downloadUrl; }
    public void setSsid(String ssid) { this.ssid = ssid; }
    public void setToken(String token) { this.token = token; }
    public void setValid(Boolean valid) { this.valid = valid; }
    public void setGid(Long gid) { this.gid = gid; }
    public void setProductName(String productName) { this.productName = productName; }
    public void setProductVersion(String productVersion) { this.productVersion = productVersion; }
    public void setAnimationEnabled(Boolean animationEnabled) { this.animationEnabled = animationEnabled; }
    public void setMode(String mode) { this.mode = mode; }

    @Override
    public String toString(){ return "client="+ toJson().toJSONString(); }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", "client_info");
        json.put("state", state.toString());
        json.put("message", message);
        if( _link != null ) json.put("_link", _link);

        json.put("ssid", ssid);
        json.put("valid", valid);
        json.put("user_name", userName);
        json.put("user_ip", userIp);
        json.put("gid", gid);
        json.put("download_url", downloadUrl);
        json.put("product_name", productName);
        json.put("product_version", productVersion);
        json.put("animation_enabled", animationEnabled);
        json.put("title_shown", titleShown);
        json.put("mode", mode);

        json.put("timestamp", getConnectDate());
        return json;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final ClientDto other = (ClientDto) obj;
        if ( this.token != null && other.token != null && this.token.equals(other.token) ) return true;
        if ( this.ssid != null && other.ssid != null && this.ssid.equals(other.ssid) ) return true;
//            if ( this.userIp != null && other.userIp != null && this.userIp.equals(other.userIp) ) {
//                if ( this.userName != null && other.userName != null && this.userName.equals(other.userName) )
//                    return true;
//            }
//        }
        return false;
    }

}
