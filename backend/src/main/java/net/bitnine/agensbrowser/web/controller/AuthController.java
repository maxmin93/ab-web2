package net.bitnine.agensbrowser.web.controller;

import net.bitnine.agensbrowser.web.message.ClientDto;
import net.bitnine.agensbrowser.web.message.ResponseDto;
import net.bitnine.agensbrowser.web.storage.ClientStorage;
import net.bitnine.agensbrowser.web.util.TokenUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
//import org.springframework.mobile.device.Device;

import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import reactor.core.publisher.Mono;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

@RestController
@RequestMapping(value = "${agens.api.base-path}/auth")
public class AuthController {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String txBase = "auth";
    @Value("${agens.api.base-path}")
    private String basePath;
    @Value("${agens.product.name}")
    private String productName;
    @Value("${agens.product.version}")
    private String productVersion;
    @Value("${agens.product.hello-msg}")
    private String hello_msg;
    @Value("${agens.jwt.header}")
    private String ssidHeader;

    @Autowired
    private TokenUtil jwtTokenUtil;

    @Autowired
    ClientStorage clients;

    private final HttpHeaders productHeaders(){
        HttpHeaders headers = new HttpHeaders();
        headers.add("agens.product.name", productName);
        headers.add("agens.product.version", productVersion);
        return headers;
    }

    // 권한없음 메시지 반환
    private final ResponseEntity<?> unauthorizedMessage(){
        ResponseDto response = new ResponseDto();
        response.setState(ResponseDto.StateType.FAIL);
        response.setMessage("You do not have right SESSION_ID. Do connect again");
        response.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/core/connect").replaceQuery("").toUriString());
        return new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.UNAUTHORIZED);
    }

    // token을 발급하고 ssid를 반환
    @RequestMapping(value="connect", method=RequestMethod.GET, produces="application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> connectRequest(HttpServletRequest request
                    ) throws InterruptedException {

        final String userIp = request.getRemoteAddr();
        logger.info(String.format("/%s/%s/%s?addr=%s", basePath, txBase, "connect", userIp));

        // token 생성
        ClientDto client = new ClientDto(clients.getDbUser(), userIp);
        final String token = jwtTokenUtil.generateToken(client);

        HttpStatus status = HttpStatus.SERVICE_UNAVAILABLE;
        if( token != null ){
            status = HttpStatus.OK;

            // client 등록
            client.setToken(token);
            clients.addClient(client.getSsid(), client);

            client.setValid(jwtTokenUtil.validateToken(token,client));
            client.setState(ResponseDto.StateType.SUCCESS);
            client.setMessage( hello_msg );
        }
        else{
            client.setState(ResponseDto.StateType.FAIL);
            client.setMessage("ERROR: To generate token is failed! Try again or ask to administrator.");
            client.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/core/connect").replaceQuery("").toUriString());
        }

        return Mono.just(new ResponseEntity<Object>(client.toJson(), productHeaders(), status));
    }

    // ssid에 대한 token 갱신 (createDate를 현재시간으로 바꿔 재발급)
    @RequestMapping(value="refresh", method=RequestMethod.GET, produces="application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> connectRefresh(HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "refresh", ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // token 생성
        final String token = jwtTokenUtil.refreshToken(client.getToken());

        HttpStatus status = HttpStatus.PARTIAL_CONTENT;
        if( token != null ){
            status = HttpStatus.OK;

            // client의 Token 갱신
            client.setToken(token);

            client.setValid(jwtTokenUtil.validateToken(token,client));
            client.setState(ResponseDto.StateType.SUCCESS);
            client.setMessage( hello_msg );
        }
        else{
            // 이전 토큰의 valid 검사
            client.setValid(jwtTokenUtil.validateToken(client.getToken(),client));

            client.setState(ResponseDto.StateType.FAIL);
            client.setMessage("ERROR: To refresh token is failed! Try again or start from login.");
            client.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/core/connect").replaceQuery("").toUriString());
        }

        return Mono.just(new ResponseEntity<Object>(client.toJson(), productHeaders(), status));
    }

    // 유효 ssid 인지 확인
    @RequestMapping(value="valid", method=RequestMethod.GET, produces="application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> connectValid(HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "valid", ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // token 추출 및 valid 검사
        final String token = client.getToken();
        // 토큰 유효성 검사: 1) 사용자 정보, 2) 토큰 유효기간
        client.setValid(jwtTokenUtil.validateToken(token,client));

        client.setState(ResponseDto.StateType.SUCCESS);
        client.setMessage( hello_msg );

        return Mono.just(new ResponseEntity<Object>(client.toJson(), productHeaders(), HttpStatus.OK));
    }

    // 자신의 ssid 를 해제 (IP가 같아야 valid)
    @RequestMapping(value="disconnect", method=RequestMethod.GET, produces="application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> disconnect(HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "disconnect", ssid));

        final String userIp = request.getRemoteAddr();

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null || !client.getUserIp().equals(userIp) ) return Mono.just(unauthorizedMessage());

        // ssid, client 삭제
        final Boolean done = clients.removeClient(ssid);

        ResponseDto response = new ResponseDto();
        response.setState(ResponseDto.StateType.SUCCESS);
        response.setMessage("disconnect client: ssid='"+ssid+"', done="+done);
        response.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/core/connect").replaceQuery("").toUriString());

        return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.OK));
    }

    ////////////////////////////////////////////////////////

    // for DEBUG
    //
    @RequestMapping(value="clients", method=RequestMethod.GET, produces="application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> listClients(HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "clients", ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // 현재 활성 client 목록 반환
        List<Object> jsonList = clients.getAllClients();
        return Mono.just(new ResponseEntity<List<Object>>(jsonList, productHeaders(), HttpStatus.OK));

//        return Flux.fromIterable(jsonList);
    }

}
