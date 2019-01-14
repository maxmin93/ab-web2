package net.bitnine.agensbrowser.web.controller;

import net.bitnine.agensbrowser.web.message.*;
import net.bitnine.agensbrowser.web.persistence.outer.service.GraphService;
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
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import reactor.core.publisher.Mono;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping(value = "${agens.api.base-path}/report")
public class ReportController {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String txBase = "report";
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

    @Autowired
    @Qualifier("agensExecutor")
    ThreadPoolTaskExecutor executor;

    private SchemaStorage schemaStorage;
    private ClientStorage clientStorage;
    private QueryService queryService;
    private GraphService graphService;

    @Autowired
    public ReportController(
            SchemaStorage schemaStorage,
            ClientStorage clientStorage,
            QueryService queryService,
            GraphService graphService
    ){
        this.schemaStorage = schemaStorage;
        this.clientStorage = clientStorage;
        this.queryService = queryService;
        this.graphService = graphService;
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

    @RequestMapping(value = "graph/{pid}", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> loadProject( @PathVariable Integer pid,
                                                HttpServletRequest request) throws Exception {

        final String guestKey = request.getHeader(this.ssidHeader)==null ? "agens" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s/%d?&guestKey=%s", basePath, txBase, "graph", pid, guestKey));
//        System.out.println(String.format("%s/%s/%s/%d?&guestKey=%s", basePath, txBase, "graph", pid, guestKey));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClientWithGuest(guestKey);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // graphStorage 에 addGraph 없이, pid 에 해당하는 project graph 만 출력
        CompletableFuture<GraphDto> future = graphService.loadProject(client, pid, true);
        CompletableFuture.allOf(future).join();     // wait until done

        GraphDto dto = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( dto == null ) {
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("loadProject ERROR: pid=%d not exists", pid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        return Mono.just(new ResponseEntity<List<Object>>(dto.toJsonList(), productHeaders(), HttpStatus.OK));
    }

}
