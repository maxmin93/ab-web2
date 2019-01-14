package net.bitnine.agensbrowser.web.persistence.inner.service;

import net.bitnine.agensbrowser.web.persistence.inner.dao.AgensProjectDao;
import net.bitnine.agensbrowser.web.persistence.inner.dao.AgensLogDao;
import net.bitnine.agensbrowser.web.persistence.inner.model.AgensLog;
import net.bitnine.agensbrowser.web.persistence.inner.model.AgensProject;
import net.bitnine.agensbrowser.web.storage.ClientStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@Transactional
public class AgensService {

    private final static Logger logger = LoggerFactory.getLogger(AgensService.class);

    private AgensProjectDao projectDao;
    private AgensLogDao logDao;
    private ClientStorage clients;

    @Autowired
    public AgensService(
            AgensProjectDao projectDao,
            AgensLogDao logDao,
            ClientStorage clients
    ){
        this.projectDao = projectDao;
        this.logDao = logDao;
        this.clients = clients;
    }

    //////////////////////////////////////////////////////////

    public List<AgensProject> findProjectsAll(){
        List<AgensProject> projects = projectDao.findAll(new Sort(Sort.Direction.DESC, "id"));
        return projects;
    }
    public List<AgensProject> findProjectsByUserName(){
        List<AgensProject> projects = projectDao.findListByUserName(clients.getDbUser());
        return projects;
    }
    public AgensProject findOneProjectById(Integer id){
        AgensProject project = projectDao.findOneByIdAndUserName(id, clients.getDbUser());
        return project;
    }

    // latest schema capture: description and image (saved at project.id=0)
    public AgensProject getSchemaCapture(){
        Optional<AgensProject> project = projectDao.findById(0);
        return project.isPresent() ? project.get() : null;
    }

    // project capture image
    public String findProjectImageById(Integer id){
//        Optional<AgensProject> project = projectDao.findById(id);
//        return project.isPresent() ? project.get().getImage() : null;
        List<AgensProject> images = projectDao.findImagesById(id);
        return images.size() > 0 ? (String)images.get(0).getImage() : null;
    }

    public AgensProject saveProject(AgensProject project){
        if( project == null ) return null;

        if( project.getId() != null ){
            // update project
        }
        return projectDao.saveAndFlush(project);
    }

    public Boolean deleteProject(Integer id){
        AgensProject project = projectDao.findOneByIdAndUserName(id, clients.getDbUser());
        if( project != null ){
            projectDao.delete(project);
            return true;
        }
        return false;
    }

    //////////////////////////////////////////////////////////

    public List<AgensLog> findLogsAll(){
        List<AgensLog> logs = logDao.findAll(new Sort(Sort.Direction.DESC, "id"));
        return logs;
    }
    public List<AgensLog> findLogsByUserName(){
        List<AgensLog> logs = logDao.findListByUserName(clients.getDbUser());
        return logs;
    }
    public AgensLog findOneLogById(Integer id){
        AgensLog log = logDao.findOneByIdAndUserName(id, clients.getDbUser());
        return log;
    }

    public AgensLog saveLog(String sql, String state, String message){
        if( sql == null || state == null ) return null;

        AgensLog log = new AgensLog(clients.getDbUser(), "localhost", sql, state);
        log.setMessage(message);

        return logDao.saveAndFlush(log);
    }

    public Boolean deleteLog(Integer id){
        AgensLog log = logDao.findById(id).orElse(null);
        if( log != null ){
            logDao.delete(log);
            return true;
        }
        return false;
    }

}
