package net.bitnine.agensbrowser.web.persistence.inner.dao;

import net.bitnine.agensbrowser.web.persistence.inner.model.AgensLog;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface AgensLogDao extends JpaRepository<AgensLog,Integer> {

    @Query(value = "select ID, USER_NAME, USER_IP, QUERY, STATE, MESSAGE, CREATE_DT, UPDATE_DT from AGENS_USER_LOGS where USER_NAME = ?1 order by ID desc limit 1000", nativeQuery = true)
    List<AgensLog> findListByUserName(String userName);
    AgensLog findOneByIdAndUserName(Integer id, String userName);

//    @Override
    Optional<AgensLog> findById(Integer id);
    List<AgensLog> findAll(Sort sort);
    List<AgensLog> findByUserName(Sort sort, String userName);
    List<AgensLog> findByUserNameAndUserIp(Sort sort, String userName, String userIp);

    @Override
    AgensLog save(AgensLog log);
    @Override
    AgensLog saveAndFlush(AgensLog log);
    @Override
    void flush();

    @Override
    void delete(AgensLog log);
}
