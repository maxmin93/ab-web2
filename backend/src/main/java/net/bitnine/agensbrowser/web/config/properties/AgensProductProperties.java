package net.bitnine.agensbrowser.web.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

// application.yml 파일에서 prefix에 대응되는 설정값들을 class 로 로딩하기
@ConfigurationProperties(prefix = "agens.product")
public class AgensProductProperties {

    private String name = "";
    private String version = "";
    private String helloMsg = "";

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public String getHelloMsg() { return helloMsg; }
    public void setHelloMsg(String helloMsg) { this.helloMsg = helloMsg; }

    @Override
    public String toString() {
        return "Product{" +
                "Name='" + name + '\'' +
                ", Version='" + version + '\'' +
                ", helloMsg='" + helloMsg + '\'' +
                '}';
    }
}
