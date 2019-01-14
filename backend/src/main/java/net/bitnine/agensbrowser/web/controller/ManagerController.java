package net.bitnine.agensbrowser.web.controller;

import net.bitnine.agensbrowser.web.message.ClientDto;
import net.bitnine.agensbrowser.web.message.ProjectDto;
import net.bitnine.agensbrowser.web.message.ResponseDto;
import net.bitnine.agensbrowser.web.persistence.inner.model.AgensLog;
import net.bitnine.agensbrowser.web.persistence.inner.model.AgensProject;
import net.bitnine.agensbrowser.web.persistence.inner.service.AgensService;
import net.bitnine.agensbrowser.web.storage.ClientStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import reactor.core.publisher.Mono;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping(value = "${agens.api.base-path}/manager")
public class ManagerController {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final AtomicLong txSeq = new AtomicLong(11);
    private static final String txBase = "manager";

    @Value("${agens.api.base-path}")
    private String basePath;
    @Value("${agens.outer.datasource.graph_path}")
    private String graphPath;
    @Value("${agens.product.name}")
    private String productName;
    @Value("${agens.product.version}")
    private String productVersion;
    @Value("${agens.jwt.header}")
    private String ssidHeader;

    private AgensService agensService;
    private ClientStorage clients;

    @Autowired
    public ManagerController(AgensService agensService, ClientStorage clients){
        super();
        this.agensService = agensService;
        this.clients = clients;
    }

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

    ///////////////////////////////////////////////////

    @RequestMapping(value = "projects/all",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getProjectsAll(
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "projects/all", ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        List<AgensProject> list = agensService.findProjectsAll();
        if( list == null ) return Mono.just(ResponseEntity.badRequest().headers(productHeaders()).body(null));

        return Mono.just(new ResponseEntity<List<AgensProject>>(list, productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "projects",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getProjects(
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "projects", ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        List<AgensProject> list = agensService.findProjectsByUserName();
        if( list == null ) return Mono.just(ResponseEntity.badRequest().headers(productHeaders()).body(null));

        return Mono.just(new ResponseEntity<List<AgensProject>>(list, productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "projects/{id}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getProjectById(@PathVariable Integer id,
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%d?ssid=%s", basePath, txBase, "projects", id, ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        AgensProject project = agensService.findOneProjectById(id);
        if( project == null ){
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("Project select fail: id=%d, Check this on project list", id));
            response.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/manager/projects").replaceQuery("").toUriString());
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        return Mono.just(new ResponseEntity<AgensProject>(project, productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "projects/{id}/image",
            method = RequestMethod.GET, produces = "text/plain; charset=utf-8")
    public Mono<ResponseEntity<?>> getProjectImageById(@PathVariable Integer id,
                                                  HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%d?ssid=%s", basePath, txBase, "projects", id, ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        String image = agensService.findProjectImageById(id);
        return Mono.just(new ResponseEntity<String>(image, productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "projects/delete/{id}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> deleteProjectById(@PathVariable Integer id,
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%d?ssid=%s", basePath, txBase, "projects/delete", id, ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // delete project
        Boolean result = agensService.deleteProject(id);
        ResponseDto response = new ResponseDto();

        // on fail
        if( !result ){
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("Project delete fail: id=%d, Check this project", id));
            response.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/manager/projects/"+id).replaceQuery("").toUriString());
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        // on success
        response.setState(ResponseDto.StateType.SUCCESS);
        response.setMessage(String.format("Project delete success: id=%s, Reload projects", id));
        response.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/manager/projects").replaceQuery("").toUriString());
        return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "projects/save", method = RequestMethod.POST
            , consumes="application/json; charset=utf-8", produces="application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> saveProject(@RequestBody ProjectDto dto,
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?title=%s&ssid=%s", basePath, txBase, "projects/save", dto.getTitle(), ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        // save project
        AgensProject project = new AgensProject(client, dto);
        project = agensService.saveProject(project);

        // on error
        if( project == null ){
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            String projId = String.valueOf(dto.getId());
            response.setMessage(String.format("Project save fail: id=%s, title=\"%s\". Check this project", projId, dto.getTitle()));
            response.set_link(ServletUriComponentsBuilder.fromCurrentRequest().replacePath("/"+basePath+"/manager/projects/"+projId).replaceQuery("").toUriString());
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }
        // on success
        return Mono.just(new ResponseEntity<AgensProject>(project, productHeaders(), HttpStatus.OK));
    }

    ///////////////////////////////////////////////////

    @RequestMapping(value = "logs/all",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getLogsAll(
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "logs/all", ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        List<AgensLog> list = agensService.findLogsAll();
        if( list == null ) return Mono.just(ResponseEntity.badRequest().headers(productHeaders()).body(null));

        return Mono.just(new ResponseEntity<List<AgensLog>>(list, productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "logs",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getLogs(
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "logs", ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        List<AgensLog> list = agensService.findLogsByUserName();
        if( list == null ) return Mono.just(ResponseEntity.badRequest().headers(productHeaders()).body(null));

        return Mono.just(new ResponseEntity<List<AgensLog>>(list, productHeaders(), HttpStatus.OK));
    }

    @RequestMapping(value = "logs/{id}",
            method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> getLogById(@PathVariable Integer id,
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s/%d?ssid=%s", basePath, txBase, "logs", id, ssid));

        // ssid 유효성 검사
        ClientDto client = clients.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        AgensLog log = agensService.findOneLogById(id);
        if( log == null ) return Mono.just(ResponseEntity.badRequest().headers(productHeaders()).body(null));

        return Mono.just(new ResponseEntity<AgensLog>(log, productHeaders(), HttpStatus.OK));
    }

}
