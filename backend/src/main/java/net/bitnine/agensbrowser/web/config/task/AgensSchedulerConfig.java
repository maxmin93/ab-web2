package net.bitnine.agensbrowser.web.config.task;

import net.bitnine.agensbrowser.web.persistence.outer.service.SchemaService;
import net.bitnine.agensbrowser.web.service.ClientScheduler;
import net.bitnine.agensbrowser.web.service.SchemaScheduler;

import net.bitnine.agensbrowser.web.storage.ClientStorage;
import net.bitnine.agensbrowser.web.storage.GraphStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
@EnableScheduling
public class AgensSchedulerConfig implements SchedulingConfigurer {

    private final int POOL_SIZE = 10;


    private SchemaService schemaService;
    private ClientStorage clientStorage;
    private GraphStorage graphStorage;

    @Autowired
    public AgensSchedulerConfig(SchemaService schemaService, ClientStorage clientStorage ){
        super();
        this.schemaService = schemaService;
        this.clientStorage = clientStorage;
    }

    @Bean
    @Scope("prototype")
    public SchemaScheduler metaSchedulerBean() {
        return new SchemaScheduler(schemaService);
    }

    @Bean
    @Scope("prototype")
    public ClientScheduler clientSchedulerBean() {
        return new ClientScheduler(clientStorage, graphStorage);
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar scheduledTaskRegistrar) {
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();

        threadPoolTaskScheduler.setPoolSize(POOL_SIZE);
        threadPoolTaskScheduler.setThreadNamePrefix("Agens-scheduled-");
        threadPoolTaskScheduler.initialize();

        scheduledTaskRegistrar.setTaskScheduler(threadPoolTaskScheduler);
    }

}