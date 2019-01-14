package net.bitnine.agensbrowser.web;

import net.bitnine.agensbrowser.web.config.properties.AgensFileProperties;
import net.bitnine.agensbrowser.web.config.properties.AgensClientProperties;
import net.bitnine.agensbrowser.web.config.properties.AgensProductProperties;
import net.bitnine.agensbrowser.web.persistence.outer.model.HealthType;
import net.bitnine.agensbrowser.web.persistence.outer.service.SchemaService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.env.Environment;

@SpringBootApplication
@ComponentScan
@EnableCaching
@EnableConfigurationProperties({ AgensFileProperties.class, AgensClientProperties.class, AgensProductProperties.class })
public class AgensBrowserWebApplication {

    public static void main(String[] args) {

		ConfigurableApplicationContext ctx = SpringApplication.run(AgensBrowserWebApplication.class, args);

		// ** 작동안함: pid 쓰기
		// ctx.addApplicationListener(new ApplicationPidFileWriter("./agensbrowser.pid"));

        // 서버 시작 알림
        Environment env = ctx.getEnvironment();
        String hello_msg = env.getProperty("agens.product.hello-msg");
        if( hello_msg != null ){
            System.out.println("\n==============================================");
            System.out.println(" " + hello_msg);
            System.out.println("==============================================\n");
        }

        // 서버 시작 로깅: 너무 자주해서 보기 싫음 ==> 주석처리
//        AgensService agensService = ctx.getBean(AgensService.class);
//        AgensLog log = agensService.saveLog("server start"
//                , ResultDto.StateType.PENDING.toString(), hello_msg);

		SchemaService schemaService = ctx.getBean(SchemaService.class);
        // 최초 AgensGraph version 확인
        System.out.println("check version of AgensGraph ... "+ schemaService.checkAGVersion()+"\n");
        // 최초 메타정보 로딩
        System.out.println("first loading META-info of AgensGraph starts...");
		schemaService.reloadMeta(0L);

		schemaService.updateHealthInfo();
        HealthType healthInfo = schemaService.getHealthInfo();
        // 여기서 AgensProductProperties 호출해 봐야, config 읽은 인스턴스가 아니라 공백 출력함
        // AgensProductProperties productProperties = ctx.getBean(AgensProductProperties.class);
        healthInfo.setProductName( env.getProperty("agens.product.name") );
        healthInfo.setProductVersion( env.getProperty("agens.product.version") );
        System.out.println("healthInfo ==> "+healthInfo.toString());
	}
}
