package net.bitnine.agensbrowser.web.service;

import net.bitnine.agensbrowser.web.storage.ClientStorage;

import net.bitnine.agensbrowser.web.storage.GraphStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
public class ClientScheduler {

    private static final Logger logger = LoggerFactory.getLogger(ClientScheduler.class);

    private ClientStorage clients;
    private GraphStorage graphs;

    @Autowired
    public ClientScheduler(
        ClientStorage clients,
        GraphStorage graphs
    ){
        this.clients = clients;
        this.graphs = graphs;
    }

    // 10초(1000ms * 10)마다 client 유효성 검사 (900000 ms = 15분 후부터)
    @Scheduled(fixedDelay = 10000, initialDelay = 9000000)
    public void refreshClients() {
        clients.removeInvalidClients();
    }

    // 5분(1000ms * 60 * 5)마다 오래된 TinkerGraph 정리 (10분 후부터)
    @Scheduled(fixedDelay = 300000, initialDelay = 600000)
    public void cleanOldGraphs() {
        List<Long> gids = clients.getCurrentGids();
        if( gids.size() == 0 ) return;

        // gid 최소값을 구해, 이보다 작은(오래된) graph는 storage에서 제거
        Long limitGid = Collections.min(gids);
        for( Long gid : graphs.getStorage().keySet() ) {
            if( gid < limitGid ) {
                System.out.println(String.format("ClientScheduler: Unused graph(gid=%d) is removed", gid));
                graphs.removeGraph( gid );
            }
        }
    }

}
