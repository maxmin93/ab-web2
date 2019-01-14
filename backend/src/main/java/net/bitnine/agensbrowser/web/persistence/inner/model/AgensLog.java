package net.bitnine.agensbrowser.web.persistence.inner.model;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

/*
  ID int not null,
  USER_ID varchar(100) not null,
  QUERY varchar(10000) not null default '',
  STATE varchar(2000) not null default '',
  MESSAGE varchar(40000) not null default '',
  CREATE_DT timestamp default CURRENT_TIMESTAMP,
 */
@Entity
@Table(name = "AGENS_USER_LOGS")
public class AgensLog implements Serializable {

    private static final long serialVersionUID = 2822601521889829886L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="ID", nullable=false)
    private Integer id;

    @Column(name="USER_NAME", nullable=false)
    private String userName;
    @Column(name="USER_IP", nullable=false)
    private String userIp;

    @Column(name="QUERY", nullable=false)
    private String query;
    @Column(name="STATE", nullable=false)
    private String state;
    @Column(name="MESSAGE")
    private String message;

    @CreationTimestamp
    @Column(name="CREATE_DT", insertable=true, updatable=false
            , columnDefinition="TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    private Timestamp create_dt;
    @UpdateTimestamp
    @Column(name="UPDATE_DT", insertable=false, updatable=true
            , columnDefinition="TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    private Timestamp update_dt;

    public AgensLog() { }

    public AgensLog(String userName, String userIp, String query, String state) {
        this.userName = userName;
        this.userIp = userIp;
        this.query = query;
        this.state = state;
    }

    public Integer getId() { return id; }
    public String getUserName() { return userName; }
    public String getUserIp() { return userIp; }
    public Timestamp getCreate_dt() { return create_dt; }
    public Timestamp getUpdate_dt() { return update_dt; }
    public String getQuery() { return query; }
    public String getState() { return state; }
    public String getMessage() { return message; }

    public void setId(Integer id) { this.id = id; }
    public void setUserName(String userName) { this.userName = userName; }
    public void setUserIp(String userIp) { this.userIp = userIp; }
    public void setCreate_dt(Timestamp create_dt) { this.create_dt = create_dt; }
    public void setUpdate_dt(Timestamp update_dt) { this.create_dt = update_dt; }
    public void setQuery(String query) { this.query = query; }
    public void setState(String state) { this.state = state; }
    public void setMessage(String message) { this.message = message; }

    @Override
    public String toString() {
        return "AgensLog{" +
                "id=" + id +
                ", userName=" + userName +
                ", userIp=" + userIp +
                ", query='" + query + '\'' +
                ", state='" + state + '\'' +
                ", create_dt=" + create_dt +
                ", update_dt=" + update_dt +
                ", message='" + message + '\'' +
                '}';
    }
}
