package net.bitnine.agensbrowser.web.persistence.inner.dao;

import net.bitnine.agensbrowser.web.persistence.inner.model.AgensProject;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface AgensProjectDao extends JpaRepository<AgensProject,Integer> {

//    @Query(value = "select ID, USER_NAME, USER_IP, TITLE, DESCRIPTION, CREATE_DT, UPDATE_DT, case when length(SQL)>50 then substring(SQL,1,47)||' ..' else SQL end as SQL, null as IMAGE, null as GRYO_DATA from AGENS_USER_PROJECTS where ID > 0 and USER_NAME = ?1 order by ID desc", nativeQuery = true)
    // ** Project list API
    // image는 별도의 URL 요청으로 제공 (여기서는 null 처리)
    // GRYO_DATA는 graph/load 에서 사용 (여기서는 null 처리)
    // SQL 은 사실 얼마 안되는 용량이라서 리스트 호출때 문장 전체를 반환
    @Query(value = "select ID, USER_NAME, USER_IP, TITLE, DESCRIPTION, CREATE_DT, UPDATE_DT, SQL, null as IMAGE, null as GRYO_DATA from AGENS_USER_PROJECTS where ID > 0 and USER_NAME = ?1 order by ID desc", nativeQuery = true)
    List<AgensProject> findListByUserName(String userName);

    // **NOTE: 컬럼이 하나라도 없으면 맵핑에 실패해서 오류 발생
    //    @Query(value = "select ID, IMAGE from AGENS_USER_PROJECTS where ID = ?1", nativeQuery = true)
    @Query(value = "select ID, null as USER_NAME, null as USER_IP, null as TITLE, null as DESCRIPTION, null as CREATE_DT, null as UPDATE_DT, null as SQL, IMAGE, null as GRYO_DATA from AGENS_USER_PROJECTS where ID = ?1", nativeQuery = true)
    List<AgensProject> findImagesById(Integer integer);

    AgensProject findOneByIdAndUserName(Integer integer, String userName);

    //    @Override
    Optional<AgensProject> findById(Integer id);
    List<AgensProject> findAll(Sort sort);
    List<AgensProject> findByUserName(Sort sort, String userName);
    List<AgensProject> findByUserNameAndUserIp(Sort sort, String userName, String userIp);

    @Override
    AgensProject save(AgensProject project);
    @Override
    AgensProject saveAndFlush(AgensProject project);
    @Override
    void flush();

    @Override
    void delete(AgensProject project);
}
