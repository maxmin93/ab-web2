package net.bitnine.agensbrowser.web.controller;

import net.bitnine.agensbrowser.web.message.*;
import net.bitnine.agensbrowser.web.persistence.inner.service.AgensService;
import net.bitnine.agensbrowser.web.persistence.outer.model.EdgeType;
import net.bitnine.agensbrowser.web.persistence.outer.model.GraphType;
import net.bitnine.agensbrowser.web.persistence.outer.model.NodeType;
import net.bitnine.agensbrowser.web.persistence.outer.service.GraphService;
import net.bitnine.agensbrowser.web.persistence.outer.service.QueryService;
import net.bitnine.agensbrowser.web.storage.SchemaStorage;
import net.bitnine.agensbrowser.web.util.JsonbUtil;
import org.json.simple.JSONObject;
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
import reactor.core.publisher.Mono;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping(value = "${agens.api.base-path}/test")
public class TestController {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final AtomicLong txSeq = new AtomicLong();
    private static final String txBase = "test";

    @Value("${agens.api.base-path}")
    private String basePath;
    @Value("${agens.product.name}")
    private String productName;
    @Value("${agens.product.version}")
    private String productVersion;

    @Value("${agens.api.query-timeout}")
    private Long queryTimeout;

    @Autowired
    @Qualifier("agensExecutor")
    ThreadPoolTaskExecutor executor;

    private SchemaStorage schemaStorage;
    private AgensService agensService;
    private QueryService queryService;
    private GraphService graphService;

    @Autowired
    public TestController(
            SchemaStorage schemaStorage,
            AgensService agensService,
            QueryService queryService,
            GraphService graphService
    ){
        this.schemaStorage = schemaStorage;
        this.agensService = agensService;
        this.queryService = queryService;
        this.graphService = graphService;
    }

    public final HttpHeaders productHeaders(){
        HttpHeaders headers = new HttpHeaders();
        headers.add("agens.product.name", productName);
        headers.add("agens.product.version", productVersion);
        return headers;
    }

    ///////////////////////////////////////////////////

    @RequestMapping(value = "timeout", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public @ResponseBody
    Mono<WebAsyncTask<ResponseEntity<?>>> getNowWithTimeout() {

        // 입력값 생성
        RequestDto req = new RequestDto("1234567890", -1L);
        req.setType(RequestDto.RequestType.QUERY);
        String sql = "Timeout Test : "+queryTimeout+" ms is over!";
        req.setSql( sql.trim() );

        // getNow() 실행
        Callable<ResponseEntity<?>> callableDto = new Callable<ResponseEntity<?>>() {
            @Override
            public ResponseEntity<?> call() throws Exception {
                ResponseDto dto = new ResponseDto();
                dto.setState(ResponseDto.StateType.SUCCESS);
                dto.setMessage( "RESULT: "+queryService.getNow() );
                return new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK);
            }
        };

        // onTimeout Callable
        Callable<ResponseEntity<?>> timeoutDto = new Callable<ResponseEntity<?>>() {
            @Override
            public ResponseEntity<?> call() throws Exception {
                ResponseDto res = new ResponseDto();
                res.setState(ResponseDto.StateType.FAIL);
                res.setMessage("ERROR: Timeout "+((Long)(queryTimeout/1000))+" seconds is over\nIf you want more timeout, modify agens-config.yml");
                return new ResponseEntity<Object>(res.toJson(), productHeaders(), HttpStatus.REQUEST_TIMEOUT);
            }
        };

        WebAsyncTask<ResponseEntity<?>> response = new WebAsyncTask<ResponseEntity<?>>(queryTimeout, executor, callableDto);
        response.onTimeout(timeoutDto);
        return Mono.just(response);
    }

    @RequestMapping(value = "hello", method = RequestMethod.GET, produces = "application/json")
    public Mono<ResponseEntity<?>> sayHello(
            @RequestParam(value = "name", defaultValue = "props") String name
            , HttpServletRequest request, HttpServletResponse response)
    {
        if( name == null ) return Mono.just(ResponseEntity.badRequest().headers(productHeaders()).body(null));
        logger.info(String.format("/%s/%s <-- %s", txBase, "projects", name));

        if( name.equals("node")){
            // String jsonStr = "keyword[6.5084]{\"id\": 1, \"keyword\": \"number-in-title\", \"phonetic_code\": \"N5165\"}";
            String jsonStr = "production[4.3682715]{\"id\": 3673628, \"kind\": \"video movie\", \"title\": \"Once Upon a Secretary\", \"md5sum\": \"3aba7079347b80e2fd1f7b1430174f9b\", \"full_info\": [{\"plot\": \"Nina is a porn star. Sharon is a secretary who wants more than just sex from her married boss Mr. Murphy. Meanwhile, tired Nina falls asleep before sex with her boyfriend Brad. He therefore calls in their neighbor Elaine as substitute. Nina wakes up but just joins in until falling asleep again. Pressured for a less tiresome job, Nina applies for a strip bar, but gets too comfortable with the clients. Meanwhile, Mr. Murphy rewards Sharon after asking her to pleasure two businessmen. Realizing she's getting paid for sex, Sharon accepts her coworker Steve's offer to become a porn star. Steve introduces Sharon to Nina who shows Sharon the ropes. They decide trading jobs. Brad comes working for Mr. Murphy too, to protect Nina from the boss who has to overhear the couple's own office sex. Sharon convinces Steve to get on camera with her.\"}, {\"certificates\": \"USA:X\"}, {\"color info\": \"Color\"}, {\"countries\": \"USA\"}, {\"genres\": \"Adult\"}, {\"languages\": \"English\"}, {\"locations\": \"New York City, New York, USA\"}, {\"runtimes\": \"77\"}, {\"tech info\": \"OFM:Video\"}, {\"tech info\": \"RAT:1.33 : 1\"}], \"phonetic_code\": \"O5215\", \"production_year\": 1983}";
            NodeType node = new NodeType();
            try{
                node.setValue(jsonStr);
            }catch( SQLException e ){
                System.out.println("node fail: "+e.getCause());
            }
            return Mono.just(new ResponseEntity<Object>(node.toJson(), productHeaders(), HttpStatus.OK));
        }
        else if( name.equals("props")){
            String jsonStr = "{\"id\": 3673628, \"kind\": \"video movie\", \"title\": \"Once Upon a Secretary\", \"md5sum\": \"3aba7079347b80e2fd1f7b1430174f9b\", \"full_info\": [{\"plot\": \"Nina is a porn star. Sharon is a secretary who wants more than just sex from her married boss Mr. Murphy. Meanwhile, tired Nina falls asleep before sex with her boyfriend Brad. He therefore calls in their neighbor Elaine as substitute. Nina wakes up but just joins in until falling asleep again. Pressured for a less tiresome job, Nina applies for a strip bar, but gets too comfortable with the clients. Meanwhile, Mr. Murphy rewards Sharon after asking her to pleasure two businessmen. Realizing she's getting paid for sex, Sharon accepts her coworker Steve's offer to become a porn star. Steve introduces Sharon to Nina who shows Sharon the ropes. They decide trading jobs. Brad comes working for Mr. Murphy too, to protect Nina from the boss who has to overhear the couple's own office sex. Sharon convinces Steve to get on camera with her.\"}, {\"certificates\": \"USA:X\"}, {\"color info\": \"Color\"}, {\"countries\": \"USA\"}, {\"genres\": \"Adult\"}, {\"languages\": \"English\"}, {\"locations\": \"New York City, New York, USA\"}, {\"runtimes\": \"77\"}, {\"tech info\": \"OFM:Video\"}, {\"tech info\": \"RAT:1.33 : 1\"}], \"phonetic_code\": \"O5215\", \"production_year\": 1983}";
            JSONObject props = null;
            try{
                props = JsonbUtil.parseJson(jsonStr);
            }catch( SQLException e ){
                System.out.println("props fail: "+e.getCause());
            }
            return Mono.just(new ResponseEntity<String>(props.toJSONString(), productHeaders(), HttpStatus.OK));
        }
        else if( name.equals("edge")){
            String jsonStr = "actor_in[30.2744][3.1,4.3682715]{\"md5sum\": \"072e281b4317ad0a8b0bdb459d2a2ed4\", \"role_name\": \"Bartender\", \"name_pcode_nf\": \"B6353\"}";
            EdgeType edge = new EdgeType();
            try {
                edge.setValue(jsonStr);
            }catch( SQLException e ){
                System.out.println("edge fail: "+e.getCause());
            }
            return Mono.just(new ResponseEntity<Object>(edge.toJson(), productHeaders(), HttpStatus.OK));
        }
        else if( name.equals("graph")){
            String jsonStr = "[person[3.1]{\"id\": 718, \"name\": \"A., David\", \"gender\": \"m\", \"md5sum\": \"cf45e7b42fbc800c61462988ad1156d2\", \"name_pcode_cf\": \"A313\", \"name_pcode_nf\": \"D13\", \"surname_pcode\": \"A\"}, actor_in[30.2744][3.1,4.3682715]{\"md5sum\": \"072e281b4317ad0a8b0bdb459d2a2ed4\", \"role_name\": \"Bartender\", \"name_pcode_nf\": \"B6353\"}, production[4.3682715]{\"id\": 3673628, \"kind\": \"video movie\", \"title\": \"Once Upon a Secretary\", \"md5sum\": \"3aba7079347b80e2fd1f7b1430174f9b\", \"full_info\": [{\"plot\": \"Nina is a porn star. Sharon is a secretary who wants more than just sex from her married boss Mr. Murphy. Meanwhile, tired Nina falls asleep before sex with her boyfriend Brad. He therefore calls in their neighbor Elaine as substitute. Nina wakes up but just joins in until falling asleep again. Pressured for a less tiresome job, Nina applies for a strip bar, but gets too comfortable with the clients. Meanwhile, Mr. Murphy rewards Sharon after asking her to pleasure two businessmen. Realizing she's getting paid for sex, Sharon accepts her coworker Steve's offer to become a porn star. Steve introduces Sharon to Nina who shows Sharon the ropes. They decide trading jobs. Brad comes working for Mr. Murphy too, to protect Nina from the boss who has to overhear the couple's own office sex. Sharon convinces Steve to get on camera with her.\"}, {\"certificates\": \"USA:X\"}, {\"color info\": \"Color\"}, {\"countries\": \"USA\"}, {\"genres\": \"Adult\"}, {\"languages\": \"English\"}, {\"locations\": \"New York City, New York, USA\"}, {\"runtimes\": \"77\"}, {\"tech info\": \"OFM:Video\"}, {\"tech info\": \"RAT:1.33 : 1\"}], \"phonetic_code\": \"O5215\", \"production_year\": 1983}]";
            GraphType graph = new GraphType();
            try {
                graph.setValue(jsonStr);
            }catch( SQLException e ){
                System.out.println("graph fail: "+e.getCause());
            }
            return Mono.just(new ResponseEntity<String>(graph.toString(), productHeaders(), HttpStatus.OK));
        }
        return Mono.just(new ResponseEntity<String>(name, productHeaders(), HttpStatus.OK));
    }

    ///////////////////////////////////////////////////


}
