package net.bitnine.agensbrowser.web.service;

import net.bitnine.agensbrowser.web.persistence.outer.model.HealthType;
import net.bitnine.agensbrowser.web.persistence.outer.service.SchemaService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class SchemaScheduler {

    private static final Logger logger = LoggerFactory.getLogger(SchemaScheduler.class);
    private final AtomicLong taskSeq = new AtomicLong(1L);

    private SchemaService schemaService;

    @Autowired
    public SchemaScheduler(SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    // 3분(1000ms * 60 * 3)마다 메타데이터 리로드 (5분 후부터)
    @Scheduled(fixedDelay = 180000, initialDelay = 300000)
    public void refreshMeta() {
        schemaService.reloadMeta(taskSeq.getAndIncrement());
    }

    // 10초(1000ms * 10)마다 healthInfo 갱신 (5초 후부터)
    @Scheduled(fixedDelay = 10000, initialDelay = 5000)
    public void refreshHealth() {
        schemaService.updateHealthInfo();

        HealthType healthInfo = schemaService.getHealthInfo();
        if( healthInfo.getIdleConnections() == 0 )
            System.out.println("healthInfo ==> "+healthInfo.toString());
    }

}