package net.bitnine.agensbrowser.web.persistence.inner.model;

import net.bitnine.agensbrowser.web.message.ClientDto;
import net.bitnine.agensbrowser.web.message.ProjectDto;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

/*
  ID int not null,
  USER_ID varchar(100) not null,
  TITLE varchar(500) not null,
  DESCRIPTION varchar(1000),
  CREATE_DT timestamp default CURRENT_TIMESTAMP,
  UPDATE_DT timestamp default CURRENT_TIMESTAMP,
  SQL varchar(10000) null default '',
  IMAGE blob null,
  GRAPH_JSON varchar(65535) default '{}',
 */
@Entity
@Table(name = "AGENS_USER_PROJECTS")
public class AgensProject implements Serializable {

    private static final long serialVersionUID = -2244201732018167424L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="ID", nullable=false)
    private Integer id;

    @Column(name="USER_NAME", nullable=false)
    private String userName;
    @Column(name="USER_IP", nullable=false)
    private String userIp;

    @Column(name="TITLE", nullable=false)
    private String title;
    @Column(name="DESCRIPTION")
    private String description;

    @CreationTimestamp
    @Column(name="CREATE_DT", insertable=true, updatable=false
            , columnDefinition="TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    private Timestamp create_dt;
    @UpdateTimestamp
    @Column(name="UPDATE_DT", insertable=false, updatable=true
            , columnDefinition="TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    private Timestamp update_dt;

    @Column(name="SQL")
    private String sql;

    @Lob
    @Column(name="IMAGE", nullable=true, columnDefinition="longtext")
    private String image;

    @Lob
    @Column(name="GRYO_DATA", nullable=true, columnDefinition="blob")
    private byte[] gryo_data;

    public AgensProject(){
        this.id = null;
        this.userName = null;
        this.userIp = null;
        this.title = null;
        this.description = null;
        this.sql = null;
        this.image = null;
        this.gryo_data = null;
    }
    public AgensProject(Integer id, String image) {
        this.id = id;
        this.userName = null;
        this.userIp = null;
        this.title = null;
        this.description = null;
        this.sql = null;
        this.image = image;
        this.gryo_data = null;
    }
    public AgensProject(String userName, String userIp, String title, String description, String sql) {
        this.id = null;
        this.userName = userName;
        this.userIp = userIp;
        this.title = title;
        this.description = description;
        this.sql = sql;
        this.image = null;
        this.gryo_data = null;
    }
    // constructor for save request from client
    public AgensProject(ClientDto client, ProjectDto project){
        this.id = project.getId();
        this.userName = client.getUserName();
        this.userIp = client.getUserIp();
        this.title = project.getTitle();
        this.description = project.getDescription();
        this.sql = project.getSql();
        this.image = project.getImage();
        this.gryo_data = null;  //project.getGraph();
    }

    public Integer getId() { return id; }
    public String getUserName() { return userName; }
    public String getUserIp() { return userIp; }
    public String getTitle() { return title; }
    public String getDescription() { return description; }
    public Timestamp getCreate_dt() { return create_dt; }
    public Timestamp getUpdate_dt() { return update_dt; }
    public String getSql() { return sql; }
    @Lob
    public byte[] getGryo_data() { return gryo_data; }
    @Lob
    public String getImage() { return image; }

    public void setId(Integer id) { this.id = id; }
    public void setUserName(String userName) { this.userName = userName; }
    public void setUserIp(String userIp) { this.userIp = userIp; }
    public void setTitle(String title) { this.title = title; }
    public void setDescription(String description) { this.description = description; }
    public void setCreate_dt(Timestamp create_dt) { this.create_dt = create_dt; }
    public void setUpdate_dt(Timestamp update_dt) { this.update_dt = update_dt; }
    public void setSql(String sql) { this.sql = sql; }
    public void setGryo_data(byte[] gryo_data) { this.gryo_data = gryo_data; }
    public void setImage(String image) { this.image = image; }

    @Override
    public String toString() {
        return "AgensProject{" +
                "id=" + id +
//                ", userName=" + userName +
//                ", userIp=" + userIp +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
//                ", create_dt=" + create_dt +
//                ", update_dt=" + update_dt +
                ", sql='" + sql + '\'' +
                ", image.size='" + ((image != null ) ? image.length() : 0) + '\'' +
                ", gryo_data.size='" + ((gryo_data != null ) ? gryo_data.length : 0) + '\'' +
                '}';
    }
}
