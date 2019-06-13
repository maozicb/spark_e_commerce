package dao.impl;


import dao.ITop10CategoryDAO;
import dao.domain.Top10Category;
import jdbc.JDBCHelper;
import scala.Serializable;

/**
 * top10品类DAO实现
 * @author Erik
 *
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO , Serializable {

	public void insert(Top10Category category) {
		String sql = "insert into top10_category values(?,?,?,?,?)";
		Object[] params = new Object[]{
				category.getTaskid(),
				category.getCategoryid(),
				category.getClickCount(),
				category.getOrderCount(),
				category.getPayCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
		
	}

}
