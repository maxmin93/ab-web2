package net.bitnine.agensbrowser.web.util;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import net.bitnine.agensbrowser.web.message.ClientDto;
import org.springframework.beans.factory.annotation.Value;
//import org.springframework.mobile.device.Device;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component
public class TokenUtil implements Serializable {

    private static final long serialVersionUID = -3301605591108950415L;

    static final String CLAIM_KEY_USERNAME = "sub";
    static final String CLAIM_KEY_USERIP = "addr";
    static final String CLAIM_KEY_AUDIENCE = "audience";
    static final String CLAIM_KEY_CREATED = "created";

//    private static final String AUDIENCE_UNKNOWN = "unknown";
//    private static final String AUDIENCE_WEB = "web";
//    private static final String AUDIENCE_MOBILE = "mobile";
//    private static final String AUDIENCE_TABLET = "tablet";

    @Value("${agens.jwt.secret}")
    private String secret;

    @Value("${agens.jwt.expiration}")
    private Long expiration;

    public String getUserNameFromToken(String token) {
        String userName;
        try {
            final Claims claims = getClaimsFromToken(token);
            userName = claims.getSubject();
        } catch (Exception e) {
            userName = null;
        }
        return userName;
    }
    public String getUserIpFromToken(String token) {
        String userIp;
        try {
            final Claims claims = getClaimsFromToken(token);
            userIp = new String((String) claims.get(CLAIM_KEY_USERIP));
        } catch (Exception e) {
            userIp = null;
        }
        return userIp;
    }
    public Date getCreatedDateFromToken(String token) {
        Date created;
        try {
            final Claims claims = getClaimsFromToken(token);
            created = new Date((Long) claims.get(CLAIM_KEY_CREATED));
        } catch (Exception e) {
            created = null;
        }
        return created;
    }
    public Date getExpirationDateFromToken(String token) {
        Date expiration;
        try {
            final Claims claims = getClaimsFromToken(token);
            expiration = claims.getExpiration();
        } catch (Exception e) {
            expiration = null;
        }
        return expiration;
    }

    public String getAudienceFromToken(String token) {
        String audience;
        try {
            final Claims claims = getClaimsFromToken(token);
            audience = (String) claims.get(CLAIM_KEY_AUDIENCE);
        } catch (Exception e) {
            audience = null;
        }
        return audience;
    }
    private Claims getClaimsFromToken(String token) {
        Claims claims;
        try {
            claims = Jwts.parser()
                    .setSigningKey(secret)
                    .parseClaimsJws(token)
                    .getBody();
        } catch (Exception e) {
            claims = null;
        }
        return claims;
    }

    // 토큰 유효기간 : 기본 1 Day
    private Date generateExpirationDate() {
        return new Date(System.currentTimeMillis() + expiration * 1000);
    }
    // 토큰 유효기간 검사 : 시스템 현재시간보다 이전인지
    private Boolean isTokenExpired(String token) {
        final Date expiration = getExpirationDateFromToken(token);
        return expiration.before(new Date());
    }

    // 토큰 생성 <== userName, userIp, Device, 현재시간
    public String generateToken(ClientDto client) {
        Map<String, Object> claims = new HashMap<>();
        claims.put(CLAIM_KEY_USERNAME, client.getUserName());
        claims.put(CLAIM_KEY_USERIP, client.getUserIp());
//        claims.put(CLAIM_KEY_AUDIENCE, generateAudience(client.getDevice()) );
        claims.put(CLAIM_KEY_CREATED, new Date());
        return generateToken(claims);
    }
    // 토큰 생성 : 생성정보와 함께 유효기간과 암호화키로 암호화하고 압축
    String generateToken(Map<String, Object> claims) {
        return Jwts.builder()
                .setClaims(claims)
                .setExpiration(generateExpirationDate())
                .signWith(SignatureAlgorithm.HS512, secret)
                .compact();
    }

    // 토큰 갱신 : Claims를 다시 추출해서 생성일자만 바꿔서 토큰 생성
    public String refreshToken(String token) {
        String refreshedToken;
        try {
            final Claims claims = getClaimsFromToken(token);
            claims.put(CLAIM_KEY_CREATED, new Date());
            refreshedToken = generateToken(claims);
        } catch (Exception e) {
            refreshedToken = null;
        }
        return refreshedToken;
    }

    // 토큰 유효성 검사: 1) 사용자 정보, 2) 토큰 유효기간
    public Boolean validateToken(String token, ClientDto client) {
        final String userName = getUserNameFromToken(token);
        final String userIp = getUserIpFromToken(token);
        final String audience = getAudienceFromToken(token);
        //final Date created = getCreatedDateFromToken(token);

        // 검사항목 : userName, userIp, device, expiredDate
        if( client.getUserName().equals(userName) && client.getUserIp().equals(userIp) ){
//                && generateAudience(client.getDevice()).equals(audience)){
            if( !isTokenExpired(token) ){
                return true;
            }
        }

        return false;
    }

/*
    // **필요없음 : 패스워드 유효기간 정책 사용시 소용되는 것
    private Boolean isCreatedBeforeLastPasswordReset(Date created, Date lastPasswordReset) {
        return (lastPasswordReset != null && created.before(lastPasswordReset));
    }

    // **필요없음 : 모바일 환경이면 토큰 유효기간 정책을 사용 안할 경우에 소용되는 것
    private Boolean ignoreTokenExpiration(String token) {
        String audience = getAudienceFromToken(token);
        return (AUDIENCE_TABLET.equals(audience) || AUDIENCE_MOBILE.equals(audience));
    }

    // ** 필요없음 : 패스워드 만료전인 경우 토큰 리프레쉬 가능 (패스워드 유효기간 정책의 사용자관리인 경우)
    // with 토큰이 아직 만료되지 않았거나 모바일/태블릿인 경우이면서
    public Boolean canTokenBeRefreshed(String token, Date lastPasswordReset) {
        final Date created = getCreatedDateFromToken(token);
        return !isCreatedBeforeLastPasswordReset(created, lastPasswordReset)
                && (!isTokenExpired(token) || ignoreTokenExpiration(token));
    }

    // 사용자 환경 : 일반 웹, 모바일 또는 태블릿
    private static final String generateAudience(SitePreference sitePreference) {
        String audience = AUDIENCE_UNKNOWN;
        if (sitePreference.isNormal()) {
            audience = AUDIENCE_WEB;
        } else if (sitePreference.isTablet()) {
            audience = AUDIENCE_TABLET;
        } else if (sitePreference.isMobile()) {
            audience = AUDIENCE_MOBILE;
        }
        return audience;
    }
*/
}