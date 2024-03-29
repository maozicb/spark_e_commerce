package dao.impl;


import dao.ISessionRandomExtractDAO;
import dao.domain.SessionRandomExtract;
import jdbc.JDBCHelper;

/**
 * 随机抽取session的DAO实现
 * @author Erik
 *
 */
public class SessinoRandomExtractDAOImpl implements ISessionRandomExtractDAO {

	//插入session随机抽取
	public void insert(SessionRandomExtract sessionRandomExtract) {
		String sql = "insert into session_random_extract values(?,?,?,?,?)";
		
		Object[] params = new Object[]{sessionRandomExtract.getTaskid(),
				sessionRandomExtract.getSessionid(),
				sessionRandomExtract.getStartTime(),
				sessionRandomExtract.getSearchKeywords(),
				sessionRandomExtract.getClickCategoryIds()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
