package net.bitnine.agensbrowser.web.controller;

import net.bitnine.agensbrowser.web.message.*;
import net.bitnine.agensbrowser.web.persistence.inner.model.AgensProject;
import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping(value = "${agens.api.base-path}/graph")
public class GraphController {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String txBase = "graph";
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
    public GraphController(
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

    @RequestMapping(value = "{gid}", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getGraph(
            @PathVariable Long gid, HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%d?ssid=%s", basePath, txBase, gid, ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<GraphType> future = graphService.getGraph(gid);
        CompletableFuture.allOf(future).join();     // wait until done

        GraphType graph = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( graph == null ) {
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("getGraph ERROR: gid=%d not exists", gid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        GraphDto dto = new GraphDto(gid, graph);
        dto.setState(ResponseDto.StateType.SUCCESS);
        String message = String.format("getGraph: gid=%d, labels=%d, nodes=%d, edges=%d"
                , gid, graph.getLabels().size(), graph.getNodes().size(), graph.getEdges().size());
        dto.setMessage(message);

        return Mono.just(new ResponseEntity<List<Object>>(dto.toJsonList(), productHeaders(), HttpStatus.OK));
    }

    // 그래프DB에 SQL 질의
    @RequestMapping(value = "schema/{gid}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getSchema( @PathVariable Long gid,
            HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s/%d?ssid=%s", basePath, txBase, "schema", gid, ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<GraphType> future = graphService.getMetaGraph(gid);
        CompletableFuture.allOf(future).join();     // wait until done

        GraphType graph = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( graph == null ) {
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("graph.schema ERROR: gid=%d not exists", gid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        GraphDto dto = new GraphDto(gid, graph);
        dto.setState(ResponseDto.StateType.SUCCESS);
        String message = String.format("graph.schema: gid=%d, labels=%d, nodes=%d, edges=%d"
                , gid, graph.getLabels().size(), graph.getNodes().size(), graph.getEdges().size());
        dto.setMessage(message);

        return Mono.just(new ResponseEntity<List<Object>>(dto.toJsonList(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "update/{gid}/{oper}",
            method = RequestMethod.POST, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> updateGraph(@PathVariable Long gid, @PathVariable String oper,
                                            @RequestBody final GraphDto data, HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader) == null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s/%d?oper=%s&nodes=%d&edges=%d&ssid=%s", basePath, txBase, "update"
                , gid, oper, data.getGraph().getNodes().size(), data.getGraph().getEdges().size(), ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<String> future = graphService.updateGraph(gid, oper, data.getGraph());
        CompletableFuture.allOf(future).join();     // wait until done

        String result = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        ResponseDto response = new ResponseDto();
        // on fail
        if( result == null ) {
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("updateGraph FAIL: gid=%d", gid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.METHOD_NOT_ALLOWED));
        }

        // on success
        response.setState(ResponseDto.StateType.SUCCESS);
        response.setMessage( result );
        return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "update-prop/{gid}/{egrp}/{eid}/{oper}",
            method = RequestMethod.POST, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> updateGraph(
                @PathVariable Long gid, @PathVariable String oper,
                @PathVariable String egrp, @PathVariable String eid,
                @RequestBody final List<Map<String,Object>> props, HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader) == null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s/%d?grp=%s&id=%s&oper=%s&props=%d&ssid=%s", basePath, txBase, "update-prop"
                , gid, egrp, eid, oper, props.size(), ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<String> future = graphService.updateGraphElement(gid, egrp, eid, oper, props);
        CompletableFuture.allOf(future).join();     // wait until done

        String result = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        ResponseDto response = new ResponseDto();
        // on fail
        if( result == null ) {
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("updateGraphElement FAIL: gid=%d, target=%s[%s] => %s(%d)", gid, egrp, eid, oper, props.size()));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.METHOD_NOT_ALLOWED));
        }

        // on success
        response.setState(ResponseDto.StateType.SUCCESS);
        response.setMessage( result );
        return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "save/{gid}", method = RequestMethod.POST)
    //, consumes = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> saveProject(@PathVariable Long gid,
                       @RequestBody final ProjectDto data,
                       HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader) == null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s/%d?nodes=%d&edges=%d&ssid=%s", basePath, txBase, "save", gid
                , data.getGraph().getNodes().size(), data.getGraph().getEdges().size(), ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<AgensProject> future = graphService.saveProject(client, gid, data);
        CompletableFuture.allOf(future).join();     // wait until done

        AgensProject result = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        // on fail
        if( result == null ) {
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("saveProject FAIL: gid=%d", gid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.METHOD_NOT_ALLOWED));
        }

        // for DEBUG
        System.out.println( String.format("saveProject[%d]: gid=%d, image.size=%d, gryo.size=%d",
                result.getId(), gid, result.getImage().length(), result.getGryo_data().length) );
        // byte array : too long and don't need
        result.setGryo_data(null);
        result.setImage(null);

        // on success
        return Mono.just(new ResponseEntity<Object>(result, productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "match/{pid}/test", method = RequestMethod.POST, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> matchProjectTest(
            @PathVariable Integer pid, @RequestBody final List<String> ids,
            HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s/%d?ids.size=%d&ssid=%s", basePath, txBase, "match(test)", pid, ids.size(), ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<ResponseDto> future = graphService.matchProjectTest(pid, ids);
        CompletableFuture.allOf(future).join();     // wait until done

        ResponseDto dto = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( dto == null ) {
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("matchProjectTest: pid=%d match test fail!", pid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        return Mono.just(new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "load/{pid}", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> loadProject( @PathVariable Integer pid,
            @RequestParam(value = "onlyData", required=false, defaultValue = "false") Boolean onlyData,
            HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s/%d?onlyData=%b&ssid=%s", basePath, txBase, "load", pid, onlyData, ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<GraphDto> future = graphService.loadProject(client, pid, onlyData);
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

    @RequestMapping(value = "new",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> newGraph(HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "new", ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // delete project
        CompletableFuture<GraphDto> future = graphService.newGraph(client);
        CompletableFuture.allOf(future).join();     // wait until done

        GraphDto result = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        // on fail
        if( result == null ){
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("Graph create fail: newGraph"));
            response.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/"+txBase+"/new").replaceQuery("").toUriString());
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        // on success
        return Mono.just(new ResponseEntity<Object>(result.toJsonList(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "delete/{gid}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> deleteGraphById(@PathVariable Long gid,
                                                   HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%d?ssid=%s", basePath, txBase, "delete", gid, ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // delete project
        CompletableFuture<Boolean> future = graphService.deleteGraph(gid);
        CompletableFuture.allOf(future).join();     // wait until done

        Boolean result = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        ResponseDto response = new ResponseDto();
        // on fail
        if( !result ){
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("Graph delete fail: gid=%d, Don't care memory", gid));
            response.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/"+txBase+"/"+gid).replaceQuery("").toUriString());
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        // on success
        response.setState(ResponseDto.StateType.SUCCESS);
        response.setMessage(String.format("Graph delete success: gid=%s, Requery another SQL", gid));
        response.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/"+txBase+"/list").replaceQuery("").toUriString());
        return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "write/{gid}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> writeGraphById(@PathVariable Long gid,
                                                   HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%d?ssid=%s", basePath, txBase, "write", gid, ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // delete project
        CompletableFuture<Boolean> future = graphService.writeGraph(gid);
        CompletableFuture.allOf(future).join();     // wait until done

        Boolean result = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        ResponseDto response = new ResponseDto();
        // on fail
        if( !result ){
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("Graph write fail: gid=%d, check download-dir", gid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        // on success
        response.setState(ResponseDto.StateType.SUCCESS);
        response.setMessage(String.format("Graph write success: gid=%s, find file in CONFIG.write_path", gid));
        return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "list",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getList(HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "list", ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<List<List<Object>>> future = graphService.getList();
        CompletableFuture.allOf(future).join();     // wait until done

        List<List<Object>> result = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( result == null ){
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("GraphList fail. sorry~"));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        DoubleListDto dto = new DoubleListDto("list", result);
        dto.setState(ResponseDto.StateType.SUCCESS);
        dto.setMessage(String.format("GraphList success: graph count=%d", result.size()));
        return Mono.just(new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK));
    }

    //////////////////////////////////////////////////////////////

    @RequestMapping(value = "filterby-groupby/{gid}",
            method = RequestMethod.POST, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> groupBy1Graph(@PathVariable Long gid,
             @RequestBody final FiltersGroupsDto params,
             HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader) == null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s/%d?filters=%s&groups=%s&ssid=%s", basePath, txBase, "filterby-groupby"
                , gid, params.getFilters().toString(), params.getGroups().toString(), ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<GraphType> future = graphService.filterNgroupBy(gid, params);
        CompletableFuture.allOf(future).join();     // wait until done

        GraphType graph = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( graph == null ) {
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("getGroupByGraph ERROR: gid=%d not exists", gid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        GraphDto dto = new GraphDto(gid, graph);
        dto.setState(ResponseDto.StateType.SUCCESS);
        String message = String.format("getGroupByGraph: gid=%d, filters=%s, groups=%s, nodes=%d, edges=%d"
                , gid, params.getFilters().toString(), params.getGroups().toString()
                , graph.getNodes().size(), graph.getEdges().size());
        dto.setMessage(message);

        return Mono.just(new ResponseEntity<List<Object>>(dto.toJsonList(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "groupby/{gid}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> groupByGraph( @PathVariable Long gid,
             @RequestParam(value="label", required=true) String label,
             @RequestParam(value="props", defaultValue="") String[] props, // List of properties
             HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("%s/%s/%s/%d?label=%s&props=%s&ssid=%s", basePath, txBase, "groupby"
                , gid, label, String.join(",", props), ssid));

        // ssid 유효성 검사: if not, send unauthorizedMessage
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<GraphType> future = graphService.getGroupByGraph(gid, label, props);
        CompletableFuture.allOf(future).join();     // wait until done

        GraphType graph = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( graph == null ) {
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("getGroupByGraph ERROR: gid=%d not exists", gid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        GraphDto dto = new GraphDto(gid, graph);
        dto.setState(ResponseDto.StateType.SUCCESS);
        String message = String.format("getGroupByGraph: gid=%d, label=%s, props='%s', nodes=%d, edges=%d"
                , gid, label, String.join(",", props), graph.getNodes().size(), graph.getEdges().size());
        dto.setMessage(message);

        return Mono.just(new ResponseEntity<List<Object>>(dto.toJsonList(), productHeaders(), HttpStatus.OK));
    }
    //////////////////////////////////////////////////////////////

    @RequestMapping(value = "findspath/{gid}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> findShortestPath( @PathVariable Long gid,
            @RequestParam(value = "sid", required=true) String sid,
            @RequestParam(value = "eid", required=true) String eid,
            HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%d?sid=%s&tid=%s&ssid=%s", basePath, txBase, "findspath", gid, sid, eid, ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<List<List<Object>>> future = graphService.findShortestPath(gid, sid, eid);
        CompletableFuture.allOf(future).join();     // wait until done

        List<List<Object>> result = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( result == null ){
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("FindShortestPath fail: gid=%d, start=%s, end=%s", gid, sid, eid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        DoubleListDto dto = new DoubleListDto("findspath", result, gid, sid, eid);
        dto.setState(ResponseDto.StateType.SUCCESS);
        dto.setMessage(String.format("FindShortestPath success: gid=%d, start=%s, end=%s, results=%d"
                , gid, sid, eid, result.size()));
        return Mono.just(new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "findcgroup/{gid}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> findConnectedGroup(
            @PathVariable Long gid, HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%d?ssid=%s", basePath, txBase, "findspath", gid, ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<List<List<Object>>> future = graphService.findConnectedGroup(gid);
        CompletableFuture.allOf(future).join();     // wait until done

        List<List<Object>> result = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( result == null ){
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("findConnectedGroup fail: gid=%d", gid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        DoubleListDto dto = new DoubleListDto("findcgroup", result, gid);
        dto.setState(ResponseDto.StateType.SUCCESS);
        dto.setMessage(String.format("findConnectedGroup success: gid=%d, results=%d", gid, result.size()));
        return Mono.just(new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "findcycles/{gid}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> findCycles( @PathVariable Long gid, HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%d?ssid=%s", basePath, txBase, "findcycles", gid, ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<List<List<Object>>> future = graphService.findCycleDetection(gid);
        CompletableFuture.allOf(future).join();     // wait until done

        List<List<Object>> result = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( result == null ){
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("cycleDetection fail: gid=%d", gid));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        DoubleListDto dto = new DoubleListDto("findcycles", result, gid);
        dto.setState(ResponseDto.StateType.SUCCESS);
        dto.setMessage(String.format("cycleDetection success: gid=%d, results=%d", gid, result.size()));
        return Mono.just(new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK));
    }

    //////////////////////////////////////////////////////////////

    @RequestMapping(value = "propstat/{gid}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getPropStats( @PathVariable Long gid,
            @RequestParam(value = "group", required=true) String group,
            @RequestParam(value = "label", required=true) String label,
            @RequestParam(value = "prop", required=true) String prop,
            @RequestParam(value = "binCount", required=false, defaultValue="50") Integer binCount,
                                        HttpServletRequest request) throws Exception {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%d?group=%s&label=%s&prop=%s&ssid=%s",
                basePath, txBase, "propstat", gid, group, label, prop, ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<PropStatDto> future = graphService.getPropStats(gid, group, label, prop, binCount);
        CompletableFuture.allOf(future).join();     // wait until done

        PropStatDto dto = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        return Mono.just(new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK));
    }
}
