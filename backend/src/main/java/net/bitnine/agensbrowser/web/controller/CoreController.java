package net.bitnine.agensbrowser.web.controller;

import net.bitnine.agensbrowser.web.message.*;
import net.bitnine.agensbrowser.web.persistence.inner.model.AgensProject;
import net.bitnine.agensbrowser.web.persistence.inner.service.AgensService;
import net.bitnine.agensbrowser.web.persistence.outer.model.HealthType;
import net.bitnine.agensbrowser.web.persistence.outer.model.type.PgprocType;
import net.bitnine.agensbrowser.web.persistence.outer.service.SchemaService;
import net.bitnine.agensbrowser.web.persistence.outer.service.QueryService;
import net.bitnine.agensbrowser.web.storage.ClientStorage;
import net.bitnine.agensbrowser.web.storage.SchemaStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.WebAsyncTask;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import reactor.core.publisher.Mono;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping(value = "${agens.api.base-path}/core")
public class CoreController {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final AtomicLong txSeq = new AtomicLong(100001);
    private static final String txBase = "core";

    @Value("${agens.api.base-path}")
    private String basePath;
    @Value("${agens.product.name}")
    private String productName;
    @Value("${agens.product.version}")
    private String productVersion;
    @Value("${agens.api.query-timeout}")
    private Long queryTimeout;
    @Value("${agens.jwt.header}")
    private String ssidHeader;
    @Value("${agens.outer.datasource.graph_path}")
    private String graphPath;

    @Autowired
    @Qualifier("agensExecutor")
    ThreadPoolTaskExecutor executor;

    private SchemaStorage schemaStorage;
    private ClientStorage clientStorage;
    private AgensService agensService;
    private QueryService queryService;
    private SchemaService schemaService;

    @Autowired
    public CoreController(
            SchemaStorage schemaStorage,
            ClientStorage clientStorage,
            AgensService agensService,
            QueryService queryService,
            SchemaService schemaService
    ){
        this.schemaStorage = schemaStorage;
        this.clientStorage = clientStorage;
        this.agensService = agensService;
        this.queryService = queryService;
        this.schemaService = schemaService;
    }

    private final HttpHeaders productHeaders(){
        HttpHeaders headers = new HttpHeaders();
        headers.add("agens.product.name", productName);
        headers.add("agens.product.version", productVersion);
        return headers;
    }

    // 권한없음 메시지 반환
    private final ResponseEntity<Object> unauthorizedMessage(){
        ResponseDto response = new ResponseDto();
        response.setState(ResponseDto.StateType.FAIL);
        response.setMessage("You do not have right SESSION_ID. Do connect again");
        response.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/core/connect").replaceQuery("").toUriString());
        return new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.UNAUTHORIZED);
    }

    ///////////////////////////////////////////////////

    // 그래프DB의 meta 정보 요청
    @RequestMapping(value = "health", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getHealth() throws Exception {
        HealthType dto = schemaStorage.getHealthInfo();
        dto.setActiveSessions( clientStorage.getActiveClientsCount() );
        dto.setProductName( productName );
        dto.setProductVersion( productVersion );
        AgensProject project = agensService.getSchemaCapture();
        dto.setDescription( project != null ? project.getDescription() : null );
        dto.setSchemaImage( project != null ? project.getImage() : null );
        return Mono.just(new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK));
    }

    // 그래프DB의 meta 정보 요청
    @RequestMapping(value = "schema", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getSchema(
            @RequestParam(value = "from", required=false, defaultValue="copy") String from,
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "schema", ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // 실행 : storageCopy 으로부터 가져온다!
        SchemaDto dto = schemaStorage.getSchemaDtoCopy();
        if( from.equals("source") ) dto = schemaStorage.getSchemaDto();     // 확인용(Verify)

        return Mono.just(new ResponseEntity<List<Object>>(dto.toJsonList(), productHeaders(), HttpStatus.OK));
    }

    // Pg procedure detail
    @RequestMapping(value = "pgproc/{pid}", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getPgprocList(@PathVariable String pid
                        , HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%s?ssid=%s", basePath, txBase, "pgproc", pid, ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        PgprocType dto = schemaService.getPgprocDetail(pid);

        return Mono.just(new ResponseEntity<PgprocType>(dto, productHeaders(), HttpStatus.OK));
    }

    // Pg procedure list
    @RequestMapping(value = "pgproc/list", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getPgprocList(HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "pgproc/list", ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // 실행 : storageCopy 으로부터 가져온다!
        List<PgprocType> dto = schemaService.getPgprocList();
        if( dto == null ) dto = new ArrayList<PgprocType>();

        return Mono.just(new ResponseEntity<List<PgprocType>>(dto, productHeaders(), HttpStatus.OK));
    }

    // Pg procedure list
    @RequestMapping(value = "pglang/list", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getPglangList(HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "pglang/list", ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // 실행 : storageCopy 으로부터 가져온다!
        List<String> dto = schemaService.getPglangList();
        if( dto == null ) dto = new ArrayList<String>();

        return Mono.just(new ResponseEntity<List<String>>(dto, productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "pgproc/save",
            method = RequestMethod.POST, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> savePgproc(@RequestBody final PgprocType proc,
                                                 HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader) == null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s?name=%s&ssid=%s", basePath, txBase
                , "pgproc/save", proc.getName(), ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        ResponseDto dto = schemaService.savePgproc(proc);
        return Mono.just(new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "pgproc/delete",
            method = RequestMethod.POST, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> deletePgproc(@RequestBody final PgprocType proc,
                                              HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader) == null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s?name=%s&ssid=%s", basePath, txBase
                , "pgproc/delete", proc.getName(), ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        ResponseDto dto = schemaService.deletePgproc(proc);
        return Mono.just(new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK));
    }

    ///////////////////////////////////////////////////

    // 그래프DB에 SQL 질의
    @RequestMapping(value = "query", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<WebAsyncTask<ResponseEntity<?>>> doQuery(
            @RequestParam(value = "gid", required=true, defaultValue = "-1") Long gid,
            @RequestParam(value = "sql", required=true) String sql,
            @RequestParam(value = "options", required=false, defaultValue="") String options,
            HttpServletRequest request) throws InterruptedException {

        // sql decoding : "+", "%", "&" etc..
        try {
            sql = URLDecoder.decode(sql, StandardCharsets.UTF_8.toString());
        }catch(UnsupportedEncodingException ue){
            System.out.println("api.query: UnsupportedEncodingException => "+ue.getCause());
        }

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s?gid=%d&sql=%s&options=%s&ssid=%s", basePath, txBase, "query", gid, sql, options, ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) {
            Callable<ResponseEntity<?>> callableDto = new Callable<ResponseEntity<?>>() {
                @Override
                public ResponseEntity<?> call() throws Exception {
                    return unauthorizedMessage();
                }
            };
            return Mono.just(new WebAsyncTask<ResponseEntity<?>>(callableDto));
        }

        // 입력값 생성
        RequestDto req = new RequestDto(ssid, gid);
        req.setType(RequestDto.RequestType.QUERY);                  // QUERY
        sql = sql.trim(); if( !sql.endsWith(";") ) sql += ";";      // 끝에 ";" 없으면 붙이기
        sql = sql.replaceAll("\\s{2,}", " ");  // 중복 공백들 제거

        req.setSql( sql );
        req.setOptions( options );

        // doQuerySync 실행
        Callable<ResponseEntity<?>> callableDto = new Callable<ResponseEntity<?>>() {
            @Override
            public ResponseEntity<?> call() throws Exception {
                ResultDto dto = queryService.doQuery(req);
                client.setGid( dto.getGid() );  // 최근 gid 갱신
                return new ResponseEntity<Object>(dto.toJsonList(), productHeaders(), HttpStatus.OK);
            }
        };

        // onTimeout Callable
        Callable<ResponseEntity<?>> timeoutDto = new Callable<ResponseEntity<?>>() {
            @Override
            public ResponseEntity<?> call() throws Exception {
                ResultDto res = new ResultDto(req);
                res.setState(ResponseDto.StateType.FAIL);
                res.setMessage("ERROR: Timeout "+((Long)(queryTimeout/1000))+" seconds is over. If you want more timeout, modify agens-config.yml");
                return new ResponseEntity<Object>(res.toJsonList(), productHeaders(), HttpStatus.REQUEST_TIMEOUT);
            }
        };

        WebAsyncTask<ResponseEntity<?>> response = new WebAsyncTask<ResponseEntity<?>>(queryTimeout, executor, callableDto);
        response.onTimeout(timeoutDto);
        return Mono.just(response);
    }

    //////////////////////////////////////////////////////////////

    private String makeSql(String type, String command, String target, String options){
        // 일단은 create와 drop만 지원
        RequestDto.RequestType reqType = RequestDto.toRequestType(type);
        if( reqType.equals(RequestDto.RequestType.NONE) ) return null;

        // options도 일단은 하나만 들어온다고 하자.
        // <== 원래는 '|' 으로 split 시켜서 array 처리 하려고 했음

        // 일단은 vlabel, elabel 에 대해서만 처리
        switch (reqType){
            case CREATE:
                if( command.equalsIgnoreCase("vlabel") || command.equalsIgnoreCase("elabel") )
                    if( !target.equals("") )
                        return new String("create "+command+" if not exists "+target+";");
            case DROP:
                if( command.equalsIgnoreCase("vlabel") || command.equalsIgnoreCase("elabel") )
                    if( !target.equals("") )
                        return new String("drop "+command+" if exists "+target+";");
            case RENAME:    // **NOTE: 매뉴얼의 if exists 의 위치가 틀린 거임!
                if( command.equalsIgnoreCase("vlabel") || command.equalsIgnoreCase("elabel") )
                    if( !target.equals("") && !options.equals("") )
                        return new String("alter "+command+" if exists "+target+" rename to "+options+";");
/*
// 오류가 나지 않는 comment 명령은 아래와 같음
// ==> 번거로워서 if exists 체크 안하고 그냥 실행하는 걸로
do $$ begin	if exists (
    select l.labname from pg_catalog.ag_label l
    INNER JOIN pg_catalog.ag_graph g ON g.oid = l.graphid
    where g.graphname = 'sample01_graph' and l.labname = 'top_user'
) then comment on vlabel top_user is 'TEST comment';
end if; end $$
*/
            case COMMENT:   // ﻿comment on vlabel top_user is 'TEST comment';
                if( command.equalsIgnoreCase("vlabel") || command.equalsIgnoreCase("elabel") )
                    if( !target.equals("") && !options.equals("") )
                        return new String("comment on "+command+" "+target+" is '"+options+"';");
        }

        return null;
    }

    //
    //  AgensBrowser web 대시보드(메인)에서 label에 대한 CREATE, DROP 요청을 처리
    //  ** NOTE: 원본인 DB로부터 반영되는 변경이 아니므로 storage 갱신은 일단 Copy에 적용된다
    //      ==> 후에 스케줄링에 의해 원본 DB 내용이 읽혀지면, storageCopy에 복사된다
    //

    // 그래프DB에 SQL 질의
    @RequestMapping(value = "command", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<WebAsyncTask<ResponseEntity<?>>> doCommand(
            @RequestParam(value = "type", required=true) String type,
            @RequestParam(value = "command", required=true) String command,
            @RequestParam(value = "target", required=true) String target,
            @RequestParam(value = "options", required=false, defaultValue="") String options,
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?command=%s&options=%s&ssid=%s", basePath, txBase, "command", command, options, ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) {
            Callable<ResponseEntity<?>> callableDto = new Callable<ResponseEntity<?>>() {
                @Override
                public ResponseEntity<?> call() throws Exception {
                    return unauthorizedMessage();
                }
            };
            return Mono.just(new WebAsyncTask<ResponseEntity<?>>(callableDto));
        }

        // 입력값 생성
        RequestDto req = new RequestDto(ssid, -1L);
        type = type.trim().toUpperCase();       req.setType(type);
        command = command.trim().toLowerCase(); req.setCommand( command );
        target = target.trim().toLowerCase();   req.setTarget( target );
        options = options.trim();               req.setOptions( options );

        final String sql = makeSql(type, command, target, options);
        // Wrong command Response
        if( sql == null ){
            Callable<ResponseEntity<?>> callableDto = new Callable<ResponseEntity<?>>() {
                @Override
                public ResponseEntity<?> call() throws Exception {
                    LabelDto res = new LabelDto(req);
                    res.setState(ResponseDto.StateType.FAIL);
                    res.setMessage("ERROR: wrong command => '"+req.getType()+" "+req.getCommand()+" "+req.getTarget()+"'");
                    return new ResponseEntity<Object>(res.toJson(), productHeaders(), HttpStatus.PARTIAL_CONTENT);
                }
            };
            return Mono.just(new WebAsyncTask<ResponseEntity<?>>(callableDto));
        }
        // request에 sql 설정
        else req.setSql( sql );

        // doCommandSync 실행
        Callable<ResponseEntity<?>> callableDto = new Callable<ResponseEntity<?>>() {
            @Override
            public ResponseEntity<?> call() throws Exception {
                LabelDto dto = queryService.doCommandSync(req);
                // 쿼리 결과인 dto의 state는 하위 service->dao 단에서 설정됨
                return new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK);
            }
        };

        // onTimeout Callable
        Callable<ResponseEntity<?>> timeoutDto = new Callable<ResponseEntity<?>>() {
            @Override
            public ResponseEntity<?> call() throws Exception {
                LabelDto res = new LabelDto(req);
                res.setState(ResponseDto.StateType.FAIL);
                res.setMessage("ERROR: Timeout "+((Long)(queryTimeout/1000))+" seconds is over. If you want more timeout, modify agens-config.yml");
                return new ResponseEntity<Object>(res.toJson(), productHeaders(), HttpStatus.PARTIAL_CONTENT);
            }
        };

        WebAsyncTask<ResponseEntity<?>> response = new WebAsyncTask<ResponseEntity<?>>(queryTimeout, executor, callableDto);
        response.onTimeout(timeoutDto);
        return Mono.just(response);
    }

}
