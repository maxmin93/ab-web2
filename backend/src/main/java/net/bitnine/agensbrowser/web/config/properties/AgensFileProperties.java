package net.bitnine.agensbrowser.web.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

// application.yml 파일에서 prefix에 대응되는 설정값들을 class 로 로딩하기
@ConfigurationProperties(prefix = "agens.file")
public class AgensFileProperties {
    private String uploadDir;
    private String downloadDir;

    public String getUploadDir() {
        return uploadDir;
    }
    public void setUploadDir(String uploadDir) {
        this.uploadDir = uploadDir;
    }

    public String getDownloadDir() {
        return downloadDir;
    }
    public void setDownloadDir(String downloadDir) {
        this.downloadDir = downloadDir;
    }
}
