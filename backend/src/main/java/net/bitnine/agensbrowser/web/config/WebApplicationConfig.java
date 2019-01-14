package net.bitnine.agensbrowser.web.config;

//import org.h2.server.web.WebServlet;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
 

@Configuration
@EnableWebMvc
public class WebApplicationConfig extends WebMvcConfigurerAdapter {
	
	private static final String[] CLASSPATH_RESOURCE_LOCATIONS = {
			"classpath:/META-INF/resources/", "classpath:/resources/",
			"classpath:/static/", "classpath:/public/" };
	
//    @Bean
//    ServletRegistrationBean h2servletRegistration(){
//        ServletRegistrationBean registrationBean = new ServletRegistrationBean( new WebServlet());
//        registrationBean.addUrlMappings("/h2-console/*");
//        return registrationBean;
//    }

    /*
     * 참고 https://spring.io/blog/2015/06/08/cors-support-in-spring-framework
     */
	@Override
	public void addCorsMappings(CorsRegistry registry) {

		// 일단 모두 해제 상태로 개발하다가 추후 클라이언트의 접근 URL 기준으로 조정 
		registry.addMapping("/**");

//		registry.addMapping("/api/**")
//		.allowedOrigins("http://domain2.com")
//		.allowedMethods("PUT", "DELETE")
//		.allowedHeaders("header1", "header2", "header3")
//		.exposedHeaders("header1", "header2")
//		.allowCredentials(false).maxAge(3600);		
	}

	// index.html을 찾기 위한 리소스 로케이션 등록 
	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry) {

//		registry.addResourceHandler("/index.html").addResourceLocations("classpath:/static/index.html");
		
		if (!registry.hasMappingForPattern("/**")) {
			registry.addResourceHandler("/**").addResourceLocations("classpath:/static/");
//					CLASSPATH_RESOURCE_LOCATIONS);
		}
	}
	
	@Override
	public void addViewControllers(ViewControllerRegistry registry) {
		// **NOTE: forward도 redirect도 안먹힘 
    	// registry.addViewController("/").setViewName("redirect:/index.html");

    	registry.addRedirectViewController("/", "/index.html");
	}
	
}
