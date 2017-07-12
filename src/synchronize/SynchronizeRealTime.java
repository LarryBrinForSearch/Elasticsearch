package synchronize;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.Client;

import connectES.ElasticSearchConnectTools;
import connectES.ElasticSearchDeleteTools;
import connectES.ElasticSearchUpdateTools;
import dbOperations.DBConnect;

/**
 * MySQL to ES 更新删除实时同步实现类
 * @author MrSong
 *
 */
public class SynchronizeRealTime {

	private String url;
	private String name;
	private String user;
	private String password;
	
	private Log lBLog = LogFactory.getLog(this.getClass());//日志 
    private int idOld,idNew,tarId,type;//分别对应监控表中的上次最大id，当前最大id，以及对应的行数id
    private String sql_MaxId,sql_getTarId;//得到最大id，tarId的sql语句
    private DBConnect db = null;//数据库操作类对象
    private ResultSet rs;//查询结果集对象
	
	ElasticSearchConnectTools esconnect = new ElasticSearchConnectTools();
	Client client = null;
	/**
	 * 构造方法
	 * @param url
	 * @param name
	 * @param user
	 * @param password
	 */
	protected SynchronizeRealTime(String url, String name, String user, String password, String tableName) {
		super();
		this.url = url;
		this.name = name;
		this.user = user;
		this.password = password;
//		sql_MaxId = "select * from "+tableName+" where id = select Max(id) from "+tableName;
		sql_MaxId = "select Max(id) from "+tableName;
		//连接数据库和ES
		startDB();
		esconnect.connectToES();
		client = esconnect.getClient();
		idOld = getMaxId();
	}
	
	/**
	 * 开始进行更新和删除的同步
	 * @param tableName
	 */
	protected void startSyn() {
		while(true) {
			getMaxId();
			if(idOld < idNew) {
				for(int i = idOld+1; i <= idNew; i++) {
					//调用ES模块方法将对应id数据进行同步
					setTarIdAndType(i);
					if(type == 0) {//插入
						System.out.println("实时同步第"+tarId+"条数据（插入）到ES");
						
					}else if(type == 1) {//更新
						System.out.println("实时同步第"+tarId+"条数据（更新）到ES");
						if(ElasticSearchUpdateTools.updateNewsById(tarId, client,db)) {
							System.out.println("更新到ES成功！");
						}else{
						System.out.println("更新到ES失败！");
						}
					}else {//删除
						System.out.println("实时同步第"+tarId+"条数据（删除）到ES");
						if(ElasticSearchDeleteTools.deleteNewsById(tarId, client)) {
							System.out.println("删除到ES成功！");
						}else{
							System.out.println("删除到ES失败！");
						}
					}
					
					
				}
			}
			idOld = idNew;
		}
	}
	
	/**
	 * 该方法反返回监控表中当前最大主键id
	 * @return
	 */
	public int getMaxId() {
		rs = db.executeQuerySQL(sql_MaxId);
		try {
			if(rs.next()) {
				idNew = rs.getInt(1);
			}else {
				lBLog.error("该表没有最大id？！");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			lBLog.error(e.getMessage());
		}
		return idNew;
	}
	
	/**
	 * 该方法获取监控表中某id对应的tarId，即内容表中修改过的id
	 * @param id
	 * @return
	 */
	public void setTarIdAndType(int id) {
		sql_getTarId = "select * from moniter where id = "+id;
		rs = db.executeQuerySQL(sql_getTarId);
		try {
			rs.next();
			tarId = rs.getInt(2);
			type = rs.getInt(3);
		}catch(SQLException e) {
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
	
}
