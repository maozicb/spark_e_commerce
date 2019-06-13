package dao;


import dao.domain.SessionDetail;

import java.util.HashSet;
import java.util.List;

/**
 * session明细接口
 *
 * @author Erik
 */
public interface ISessionDetailDAO {

    /**
     * 插入一条session明细数据
     *
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);
    void insertBatch(HashSet<SessionDetail> sessionDetails);
}
