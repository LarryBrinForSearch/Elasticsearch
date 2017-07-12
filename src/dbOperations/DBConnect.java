package dbOperations;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 

/**
 * 该类封装了对于MySql的操作
 * @author MrSong
 *
 */
public class DBConnect {
	private String url;
	private String name;
	private String user;
	private String password;
	
//    Log lBLog = LogFactory.getLog( this .getClass()); 
	
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	
	/**
	 * 构造方法，提前指定各种连接参数
	 * @param url
	 * @param name
	 * @param user
	 * @param password
	 */
	public DBConnect(String url, String name, String user, String password) {
		this.url = url;
		this.name = name;
		this.user = user;
		this.password = password;
	}
	
	/**
	 * 连接数据库
	 */
	public void connect(){
		try {  
            Class.forName(name);
            conn = DriverManager.getConnection(url, user, password);
			stmt = conn.createStatement();

//            lBLog.info("Connect MySql Success!");
            System.out.println("Connect MySql Success");
        } catch (Exception e) {  
            e.printStackTrace();
//            lBLog.error(e.getMessage());
        } 

	}

	/**
	 * MySql查询操作
	 * @param sql
	 * @return 结果集
	 */
	public ResultSet executeQuerySQL(String sql){
		try {
			rs = stmt.executeQuery(sql);
//            lBLog.info("executeQuery Success!");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
//            lBLog.error("Query Failed!");
			System.out.println("查询数据失败");
			e.printStackTrace();
		}
		return rs;
	}
	
	/**
	 * MySql插入，删除，更新，建立触发器等只需要sql语句的操作
	 * @param sql
	 */
	public void executeSQL(String sql){
		try {
		    stmt.execute(sql);
//            lBLog.info("executeSQL Success!");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
//			lBLog.error("executeSQL failed!");
			System.out.println("执行SQL失败");
			e.printStackTrace();
		}

	}
	
	/**
	 * 关闭MySql连接
	 */
	public void close() {  
        try {  
        	if(this.rs != null){
                this.rs.close();  
        	}
        	if(this.stmt != null){
                this.stmt.close();  
        	}
        	if(this.conn != null){
                this.conn.close();  
        	}
//            lBLog.info("Close MySql Success!");
            System.out.println("Close MySql Success");

        } catch (SQLException e) {
			e.printStackTrace();
//        	lBLog.error(e.getMessage());
        }  
	}  

	
}
