package net.bitnine.agensbrowser.web.storage;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Scope("singleton")
public class GraphStorage {

    private static final Logger logger = LoggerFactory.getLogger(SchemaStorage.class);

    // 쿼리에 대한 TinkerGraph 순번
    private AtomicLong gid = new AtomicLong(1000L);
    // gid에 대한 TinkerGraph 저장소
    private ConcurrentHashMap<Long, TinkerGraph> gmap = new ConcurrentHashMap<Long, TinkerGraph>();

    public Long getCurrentGid(){ return gid.get(); }
    public Map<Long, TinkerGraph> getStorage(){ return gmap; }

    public Long addGraph(TinkerGraph graph){
        Long id = gid.incrementAndGet();
        gmap.put(id, graph);
        return id;
    }

    public Boolean hasGraph(Long gid) {
        return gmap.containsKey(gid);
    }

    public TinkerGraph getGraph(Long gid){
        return gmap.get(gid);
    }
    public void setGraph(Long gid, TinkerGraph tGraph){
        removeGraph(gid);
        gmap.put(gid, tGraph);
    }

    public Boolean removeGraph(Long gid){
        if( gmap.containsKey(gid) ) {
            TinkerGraph graph = gmap.get(gid);
            if( graph != null ) graph.close();

            gmap.remove(gid);
            return true;
        }
        return false;
    }

    public List<List<Long>> getList(){
        List<List<Long>> result = new ArrayList<List<Long>>();

        for(Map.Entry<Long, TinkerGraph> iter : gmap.entrySet()){
            Long gid = iter.getKey();
            TinkerGraph graph = iter.getValue();

            List<Long> row = new ArrayList<Long>();
            row.add( gid);  // row[0] = gid

            GraphTraversalSource g = graph.traversal();
            // row[1] = label count = vertex label cnt + edge label cnt
            row.add( g.V().label().dedup().count().next() + g.E().label().dedup().count().next() );
            row.add( g.V().count().next() );    // row[2] = vertex count
            row.add( g.E().count().next() );    // row[3] = edge count

            result.add(row);
            try{ g.close(); }catch(Exception e){ System.out.println("getList: close error, gid="+gid); }
        }

        return result;
    }

}
