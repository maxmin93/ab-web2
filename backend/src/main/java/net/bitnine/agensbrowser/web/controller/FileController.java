package net.bitnine.agensbrowser.web.controller;

import net.bitnine.agensbrowser.web.message.*;
import net.bitnine.agensbrowser.web.persistence.outer.service.GraphService;
import net.bitnine.agensbrowser.web.persistence.outer.service.QueryService;
import net.bitnine.agensbrowser.web.service.FileStorageService;
import net.bitnine.agensbrowser.web.storage.ClientStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import reactor.core.publisher.Mono;

import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "${agens.api.base-path}/file")
public class FileController {

    private static final Logger logger = LoggerFactory.getLogger(FileController.class);
    private static final String txBase = "file";

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

    private FileStorageService fileStorageService;
    private ClientStorage clientStorage;
    private QueryService queryService;
    private GraphService graphService;

    @Autowired
    public FileController(
        FileStorageService fileStorageService,
        ClientStorage clientStorage,
        QueryService queryService,
        GraphService graphService
    ){
        this.fileStorageService = fileStorageService;
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
    // for TEST : hello world

    @RequestMapping(value = "hello", method = RequestMethod.GET, produces = "text/plain; charset=utf-8")
    public Mono<ResponseEntity<?>> test(
            @RequestParam(value = "value", required=false, defaultValue="World") String value,
            HttpServletRequest request) throws InterruptedException {

        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?ssid=%s", basePath, txBase, "hello", ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        String reply = "hello, "+value;
        return Mono.just(new ResponseEntity<String>(reply, productHeaders(), HttpStatus.OK));
    }

    ///////////////////////////////////////////////////

//    @GetMapping(value = "export", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
//    public @ResponseBody Mono<ResponseEntity<?>> exportFile(
//            @RequestParam("gid") Long gid,
//            @RequestParam(value="type", required=false, defaultValue="graphson") String type,
//            HttpServletRequest request
//    ) throws Exception {

    @RequestMapping(value = "export/{fileType}", method = RequestMethod.POST
                    , produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public @ResponseBody Mono<ResponseEntity<?>> exportFile(
            @PathVariable String fileType,
            @RequestBody final GraphDto data,
            HttpServletRequest request) throws Exception {
        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?fileType=%s&ssid=%s", basePath, txBase, "export", fileType, ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(new ResponseEntity<byte[]>(null, productHeaders(), HttpStatus.UNAUTHORIZED));

        CompletableFuture<ByteArrayOutputStream> future = graphService.exportGraph(fileType, data);
        CompletableFuture.allOf(future).join();     // wait until done

        ByteArrayOutputStream os = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( os == null ) return Mono.just(new ResponseEntity<byte[]>(null, productHeaders(), HttpStatus.NO_CONTENT));

        return Mono.just(new ResponseEntity<byte[]>( os.toByteArray(), productHeaders(), HttpStatus.OK));
    }

    ///////////////////////////////////////////////////

    // for importing GraphSON, GraphML
    @RequestMapping(value = "import",
            method = RequestMethod.POST, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> importFile(
            @RequestParam("gid") Long gid,
            @RequestParam("file") MultipartFile file,
            HttpServletRequest request) throws Exception {
        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?file=%s&ssid=%s", basePath, txBase, "import", file.getOriginalFilename(), ssid));

        String fileExt = file.getOriginalFilename().lastIndexOf(".") >= 0 ?
                file.getOriginalFilename().substring(file.getOriginalFilename().lastIndexOf(".")).toLowerCase() : null;
        System.out.println("importing file: '"+file.getOriginalFilename()+"' ("+fileExt+") => gid="+gid);

        // 부적절한 파일타입 FAIL 반환
        if( fileExt == null || (!fileExt.equals(".xml") && !fileExt.equals(".graphml")
                && !fileExt.equals(".json") && !fileExt.equals(".graphson"))){
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage("Importable file type are '.graphml','.xml','.graphson','.json'");
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.NOT_ACCEPTABLE));
        }

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        CompletableFuture<GraphDto> future = graphService.importGraphFile(gid, file, fileExt);
        CompletableFuture.allOf(future).join();     // wait until done

        GraphDto dto = future.get(queryTimeout, TimeUnit.MILLISECONDS);
        if( dto == null ) {
            ResponseDto response = new ResponseDto();
            response.setState(ResponseDto.StateType.FAIL);
            response.setMessage(String.format("importGraph ERROR: check file and retry!"));
            return Mono.just(new ResponseEntity<Object>(response.toJson(), productHeaders(), HttpStatus.BAD_REQUEST));
        }

        return Mono.just(new ResponseEntity<List<Object>>(dto.toJsonList(), productHeaders(), HttpStatus.OK));
    }

    // for uploading image files, etc...
    @RequestMapping(value = "upload",
            method = RequestMethod.POST, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> uploadFile(@RequestParam("file") MultipartFile file,
                                    HttpServletRequest request) {
        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?file=%s&ssid=%s", basePath, txBase, "upload", file.getOriginalFilename(), ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        UploadFileDto dto = this.uploadSingleFile(file);
        return Mono.just(new ResponseEntity<Object>(dto.toJson(), productHeaders(), HttpStatus.OK));
    }

    private UploadFileDto uploadSingleFile(MultipartFile file) {
        String fileName = fileStorageService.storeFile(file);

        String fileDownloadUri = ServletUriComponentsBuilder.fromCurrentContextPath()
                .path(String.format("/%s/%s/%s/", basePath, txBase, "download"))
                .path(fileName)
                .toUriString();

        return new UploadFileDto(fileName, fileDownloadUri,
                file.getContentType(), file.getSize());
    }

    @RequestMapping(value = "upload-multi",
            method = RequestMethod.POST, produces = "application/json; charset=utf-8")
    public Mono<ResponseEntity<?>> uploadMultipleFiles(@RequestParam("files") MultipartFile[] files,
                                    HttpServletRequest request) {
        final String ssid = request.getHeader(this.ssidHeader)==null ? "1234567890" : request.getHeader(this.ssidHeader);
        logger.info(String.format("/%s/%s/%s?files.size=%d&ssid=%s", basePath, txBase, "upload-multi", files.length, ssid));

        // ssid 유효성 검사
        ClientDto client = clientStorage.getClient(ssid);
        if( client == null ) return Mono.just(unauthorizedMessage());

        List<Object> dtos = Arrays.asList(files)
                .stream()
                .map(file -> uploadSingleFile(file).toJson())
                .collect(Collectors.toList());
        return Mono.just(new ResponseEntity<List<Object>>(dtos, productHeaders(), HttpStatus.OK));
    }

    @GetMapping("download/{fileName:.+}")
    public Mono<ResponseEntity<?>> downloadFile(@PathVariable String fileName,
                                    HttpServletRequest request) {
        logger.info(String.format("/%s/%s/%s?fileName=%s", basePath, txBase, "download", fileName));

        // Load file as Resource
        Resource resource = fileStorageService.loadFileAsResource(fileName);

        // Try to determine file's content type
        String contentType = null;
        try {
            contentType = request.getServletContext().getMimeType(resource.getFile().getAbsolutePath());
        } catch (IOException ex) {
            logger.info("downloadFile: Could not determine file type.");
        }
        // Fallback to the default content type if type could not be determined
        if(contentType == null) {
            contentType = "application/octet-stream";
        }

        return Mono.just(ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .header(HttpHeaders.CONTENT_DISPOSITION
                        , "attachment; filename=\"" + resource.getFilename() + "\"")
                .body(resource));
    }

}