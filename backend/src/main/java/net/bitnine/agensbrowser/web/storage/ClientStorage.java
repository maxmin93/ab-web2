package net.bitnine.agensbrowser.web.storage;

import net.bitnine.agensbrowser.web.config.properties.AgensClientProperties;
import net.bitnine.agensbrowser.web.config.properties.AgensProductProperties;
import net.bitnine.agensbrowser.web.message.ClientDto;

import net.bitnine.agensbrowser.web.util.TokenUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Scope("singleton")
public class ClientStorage {

    private static final Logger logger = LoggerFactory.getLogger(ClientStorage.class);

    @Value("${agens.outer.datasource.username}")
    private String dbUser;

    private final String guestKey;
    private final String productName;
    private final String productVersion;
    private final Boolean animationEnabled;
    private final Boolean titleShown;
    private final String clientMode;

    @Autowired
    private TokenUtil jwtTokenUtil;

    // Connection Map: SSID(Token), IP(userIp)
    private ConcurrentHashMap<String, ClientDto> clients;

    public ClientStorage(
            AgensClientProperties clientProperties
            , AgensProductProperties productProperties
    ) {
        this.guestKey = clientProperties.getGuestKey();
        this.productName = productProperties.getName();
        this.productVersion = productProperties.getVersion();
        this.animationEnabled = clientProperties.getAnimationEnabled();
        this.titleShown = clientProperties.getTitleShown();
        this.clientMode = clientProperties.getMode();

        clients = new ConcurrentHashMap<String, ClientDto>();
        clients.put(guestKey, new ClientDto("guest", "localhost") );

        // for DEBUG
        if( clientProperties.getMode().equals("dev") )
            clients.put("1234567890", new ClientDto("agraph", "localhost") );
    }

    public String getDbUser(){ return this.dbUser; }
    public String getProductName(){ return this.productName; }
    public String getProductVersion(){ return this.productVersion; }

    ////////////////////////////////////////

    public int getActiveClientsCount() {
        int clientsCount = 0;
        for (Map.Entry<String, ClientDto> entry : clients.entrySet()) {
            if( entry.getKey().equals("1234567890")                     // for DEBUG
                || entry.getValue().getUserName().equals("guest") ){    // for Guest
                continue;                                               // skip counting
            }
            clientsCount += 1;
        }
        return clientsCount;
    }

    public void clear(){
        clients.clear();
    }

    public Boolean addClient(String ssid, ClientDto client){

        if( ssid == null || client == null ) return false;
        if( clients.get(ssid) != null ){
            // ssid는 유일한데 같은 ssid에 다른 ip, 다른 dbUser 접속시 비정상 시도 => 거부
            if( !clients.get(ssid).equals(client) ) return false;
        }

        // 신규 client 에 환경설정 정보 갱신
        client.setProductName( productName );
        client.setProductVersion( productVersion );
        client.setAnimationEnabled( animationEnabled );
        client.setTitleShown( titleShown );
        client.setMode( clientMode );

        // 신규 client 접속 등록
        clients.put(ssid, client);
        return true;
    }

    public Boolean removeClient(String ssid){

        if( clients.get(ssid) == null ) return false;

        // 기존 client 접속 제거
        clients.remove(ssid);
        return true;
    }

    public ClientDto getClient(String ssid) {
        if( ssid == null ) return null;
        ClientDto client = clients.get(ssid);
        if( client == null ) return null;
        return client.getUserName().equals("guest") ? null : client;
    }

    public ClientDto getClientWithGuest(String ssid) {
        if( ssid == null ) return null;
        ClientDto client = clients.get(ssid);
        return client;
    }

    public List<Object> getAllClients() {
        List<Object> jsonList = new ArrayList<Object>();
        for(Map.Entry<String, ClientDto> entry : clients.entrySet())
            jsonList.add((Object)entry.getValue().toJson());
        return jsonList;
    }

    // background running by SchemaScheduler
    public void removeInvalidClients(){
        for( Map.Entry<String,ClientDto> entry : clients.entrySet() ){
//            // for DEBUG
//            if( entry.getKey().equals("1234567890") ) continue;

            // check token if valid or not
            ClientDto client = entry.getValue();

            // guestKey 에 대해서는 매 검사시마다 token 갱신 ==> 삭제로부터 회피
            if( entry.getKey().equals(guestKey) ){
                client.setToken( jwtTokenUtil.refreshToken(client.getToken()) );
                continue;   // **NOTE : 갱신을 해도 invalid 로 삭제됨 (ssid 탓?)
            }

            // token 의 ssid 정보가 일치하지 않거나 유효시간을 지난 경우 ==> invalid 제거
            if( !entry.getKey().equals(client.getSsid())
                || !jwtTokenUtil.validateToken(client.getToken(), client) ){
                // for INFO
                System.out.println("[scheduler] remove invalid client='"+entry.getKey()+"'");
                clients.remove(entry.getKey());
            }
        }
    }

    public List<Long> getCurrentGids(){
        List<Long> gids = new ArrayList<Long>();
        for( Map.Entry<String,ClientDto> entry : clients.entrySet() ) {
            Long gid = entry.getValue().getGid();
            if( gid > 0 ) gids.add( gid );
        }
        return gids;
    }

}
