package net.bitnine.agensbrowser.web.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

// application.yml 파일에서 prefix에 대응되는 설정값들을 class 로 로딩하기
@ConfigurationProperties(prefix = "agens.client")
public class AgensClientProperties {

    private String mode;
    private String guestKey;
    private Boolean animationEnabled;
    private Boolean titleShown;

    public String getMode() { return mode; }
    public String getGuestKey() {
        return guestKey;
    }
    public Boolean getAnimationEnabled() {
        return animationEnabled;
    }
    public Boolean getTitleShown() { return titleShown; }

    public void setMode(String mode) { this.mode = mode; }
    public void setGuestKey(String guestKey) {
        this.guestKey = guestKey;
    }
    public void setAnimationEnabled(Boolean animationEnabled) { this.animationEnabled = animationEnabled; }
    public void setTitleShown(Boolean titleShown) { this.titleShown = titleShown; }

}
