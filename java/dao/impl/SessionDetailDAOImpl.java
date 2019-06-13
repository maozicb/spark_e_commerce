package dao.impl;


import dao.ISessionDetailDAO;
import dao.domain.SessionDetail;
import jdbc.JDBCHelper;
import scala.Serializable;

import java.util.*;

/**
 * session明细DAO实现类
 *
 * @author Erik
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO, Serializable {

  public void   insertBatch(HashSet<SessionDetail> sessionDetails){
      String sql = "insert into session_detail value(?,?,?,?,?,?,?,?,?,?,?,?)";
      LinkedList<Object[]> arrayList = new LinkedList<>();
      for( SessionDetail sessionDetail : sessionDetails){
          Object[] params = new Object[] {
                  sessionDetail.getTaskid(),
                  sessionDetail.getUserid(),
                  sessionDetail.getSessionid(),
                  sessionDetail.getPageid(),
                  sessionDetail.getActionTime(),
                  sessionDetail.getSearchKeyword(),
                  sessionDetail.getClickCategoryId(),
                  sessionDetail.getClickProductId(),
                  sessionDetail.getOrderCategoryIds(),
                  sessionDetail.getOrderProductIds(),
                  sessionDetail.getPayCategoryIds(),
                  sessionDetail.getPayProductIds()};
          arrayList.add(params);
      }
      JDBCHelper jdbcHelper = JDBCHelper.getInstance();
      jdbcHelper.executeBatch(sql,arrayList);
  }


    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail value(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[] {
                sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);

    }
}
