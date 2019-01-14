package net.bitnine.agensbrowser.web.config.event;

import net.bitnine.agensbrowser.web.message.ResultDto;
import net.bitnine.agensbrowser.web.persistence.inner.model.AgensLog;
import net.bitnine.agensbrowser.web.persistence.inner.service.AgensService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

@Component
public class ContextClosedHandler implements ApplicationListener<ContextClosedEvent> {

    @Autowired
    @Qualifier("agensExecutor")
    ThreadPoolTaskExecutor executor;

    @Autowired
    AgensService agensService;

    // ERROR: agensScheduler 식별자를 찾지 못함. @Qualifier 없어도 마찬가지
    //
//    @Autowired
//    @Qualifier("agensScheduler")
//    ThreadPoolTaskScheduler scheduler;

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {

        String hello_msg = "AgensBrowser: background jobs shutdown";

        // **NOTE: Dao가 작동하지 않는다. 이미 죽은 상태라 그런건지..
        AgensLog log = agensService.saveLog("server stop"
                , ResultDto.StateType.KILLED.toString(), hello_msg);

        System.out.println("\n==============================================");
        System.out.println(hello_msg);

        executor.shutdown();
//        scheduler.shutdown();

        System.out.println("Bye~");
    }
}