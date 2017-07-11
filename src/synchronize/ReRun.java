package synchronize;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dbOperations.DBConnect;

/**
 * 实现重跑机制的功能类
 * @author MrSong
 *
 */
public class ReRun {

	private String url;
	private String name;
	private String user;
	private String password;
	private String tableName;

	private Log lBLog = LogFactory.getLog(this.getClass());//日志 
	private int id_begin,id_end;
    private DBConnect db = null;//数据库操作类对象
    private ResultSet rs1,rs2;//查询结果集对象
//    private ResultSet rsUI,rsD;//查询结果集对象
    private String sqlGetBegin;// = "select min(id) from moniter where date >";
    private String sqlGetEnd;// = "select max(id) from moniter where date <";

	public ReRun(String url, String name, String user, String password, String tableName) {
		super();
		this.url = url;
		this.name = name;
		this.user = user;
		this.password = password;
		
		startDB();
	}

	
	/**
	 * MySQL to ES重跑机制的调用方法
	 * @param beginTime 格式：2017-07-05 10:30:00
	 * @param endTime  格式：2017-07-05 12:30:00
	 */
	public void startReRun(String beginTime,String endTime) {
		
	}
	
	/**
	 * 获取更新和插入的tarId即website表行数
	 * @param beginTime
	 * @param endTime
	 * @return
	 */
	//select tarId from moniter where date > '2017-07-07 15:00:00';
	public ResultSet getUpdateInsetTarId(String beginTime,String endTime) {
		String sql = "select tarId from moniter where date > "+beginTime;
		ResultSet rsUI = db.executeQuerySQL(sql);
		return rsUI;
	}	
	
	/**
	 * 获取删除的tarId
	 * @param beginTime
	 * @param endTime
	 * @return
	 */
	public ResultSet getDeleteTarId(String beginTime,String endTime) {
		String sql = "select tarId from moniter where date > "+beginTime;
		ResultSet rs = db.executeQuerySQL(sql);
		return rs;
	}	

	
	public void setBeginEndId(String beginTime,String endTime) {
		
		sqlGetBegin = "select min(id) from "+tableName+" where date > "+beginTime;
		sqlGetEnd = "select max(id) from "+tableName+" where date < "+endTime;
		rs1 = db.executeQuerySQL(sqlGetBegin);
		rs2 = db.executeQuerySQL(sqlGetEnd);
		try {
			if(rs1.next()&&rs2.next()) {
				id_begin = rs1.getInt(1);
				id_end = rs2.getInt(1);
				
			}else {
				lBLog.warn("该重跑时间段内MySQL并未发生变化");
				return;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			lBLog.error(e.getMessage());
		}
	}
	
		
	/**
	 * 开启数据库连接
	 */
	protected void startDB() {
		db = new DBConnect(url, name, user, password);
		db.connect();
	}
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
