package net.bitnine.agensbrowser.web.config.task;

import java.util.concurrent.Executor;

import net.bitnine.agensbrowser.web.exception.AgensAsyncUncaughtExceptionHandler;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurerSupport;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

// Async Config는 하나만 가능

@Configuration
@EnableAsync
public class AgensAsyncExecutor extends AsyncConfigurerSupport {

    @Override
    @Bean
    @Qualifier(value = "agensExecutor")
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(7);
        executor.setMaxPoolSize(42);
        executor.setQueueCapacity(11);
        executor.setThreadNamePrefix("Agens-async-");
        executor.initialize();
        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new AgensAsyncUncaughtExceptionHandler();
    }
}