package synchronize;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.Client;

import connectES.ElasticSearchConnectTools;
import connectES.ElasticSearchDeleteTools;

import dbOperations.DBConnect;

/**
 * 实现重跑机制的功能类
 * @author MrSong
 *
 */
public class ReRun {

	
   //private String url = "jdbc:mysql://localhost:3306/crawler";
   private String url = "jdbc:mysql://117.78.37.224:3306/crawler";
   private String name = "com.mysql.jdbc.Driver";
   private String user = "root";
   private String password = "Mysql1995*";//Mysql1995*
	public static String tableName1="moniter";
	public static String  tableName2="website";
	private int MAX_THREAD=5;
	final Semaphore semaphore = new Semaphore(1);

	private Log lBLog = LogFactory.getLog(this.getClass());//日志 
	private int id_begin,id_end;
    private DBConnect db =null;//数据库操作类对象
    private Client client=null;
    private ResultSet rs1,rs2;//查询结果集对象
    private String sqlGetBegin;// = "select min(id) from moniter where date >";
    private String sqlGetEnd;// = "select max(id) from moniter where date <";
    private boolean init=false;

	public ReRun(String url, String name, String user, String password) {
		super();
		this.url = url;
		this.name = name;
		this.user = user;
		this.password = password;
		
	}
	
	public ReRun(){
		super();
	}

	
	
	public void stopReRun(){
		db.close();
		//client.close();
	}
	private void startES() {
		// TODO Auto-generated method stub
		ElasticSearchConnectTools esconnect = new ElasticSearchConnectTools();
		esconnect.connectToES();
		client=esconnect.getClient();
	}


	/**
	 * MySQL to ES重跑机制的调用方法
	 * @param beginTime 格式：2017-07-05 10:30:00
	 * @param endTime  格式：2017-07-05 12:30:00
	 * @throws SQLException 
	 */
	public void startReRun(String beginTime,String endTime) throws SQLException {
		startDB();
		//startES();  
		setBeginEndId(beginTime,endTime);
		if(init){
		startInsert();
		startDelete();
		}else{
			System.out.print("程序初始化失败，检查输入参数");
		}
			
	}
	
	
	public void startInsert(){
		//Semaphore semaphore = new Semaphore(MAX_THREAD);
		ExecutorService pool = Executors.newFixedThreadPool(MAX_THREAD);
		String sqlCount="select count(distinct tarid) count from "+tableName1+" where type!=2 and id >= "+id_begin+" and id <= "+id_end+";";
		System.out.println(sqlCount);
		String sqlGet="select distinct tarid from "+tableName1+" where type!=2 and id >="+id_begin+" and id<="+id_end+" order by tarid";
		System.out.println(sqlGet);
		ResultSet rsCount=db.executeQuerySQL(sqlCount);
		int count=0;
		try {
			if(rsCount.next()){
				count=rsCount.getInt("count");
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("error code 1");
		}
		ResultSet rsTemp=db.executeQuerySQL(sqlGet);
		System.out.println("包含"+count+"条结果");
		
		int resultTemp[]=new int[count];	
		for(int c=0;c<count;c++){
			try {
				if(rsTemp.next()){
					resultTemp[c]=rsTemp.getInt("tarid");
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		 
		List<List<Integer>> rsList=ReRunUtil.split(resultTemp);
		for(int i=0;i<rsList.size();i++){
			List<Integer> intList=rsList.get(i);
			int min=intList.get(0);
			int max=intList.get(intList.size()-1);
			InsertThread it=new InsertThread();
			it.init(min, max, db, null, semaphore);
			//it.run();
			pool.execute(it);
		}
		pool.shutdown();
		while(true){  
            if(pool.isTerminated()){  
                System.out.println("所有的子线程都结束了！");  
                break;  
            }  
        } 
	}

	
	public void startDelete(){
		startES();
		String sqlCount="select count(distinct tarid) count from "+tableName1+" where type=2 and id >= "+id_begin+" and id <="+id_end+";";
		System.out.println(sqlCount);
		String sqlGet="select distinct tarid from "+tableName1+" where type=2 and id >="+id_begin+" and id<="+id_end+" order by tarid";
		System.out.println(sqlGet);
		ResultSet rsCount=db.executeQuerySQL(sqlCount);
		int count=0;
		try {
			if(rsCount.next())
			count=rsCount.getInt("count");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("包含"+count+"条结果需要删除");
		ResultSet rsTemp=db.executeQuerySQL(sqlGet);
		//int resultTemp[]=new int[count];	
		for(int i=0;i<count;i++){
			try {
				if(rsTemp.next()){
					//resultTemp[i]=rsTemp.getInt("tarid");
					int idTemp=rsTemp.getInt("tarid");
					boolean flag=ElasticSearchDeleteTools.deleteNewsById(idTemp, client);
					if(flag==true){
						System.out.println("id"+idTemp+"删除成功");
					}else{
						System.out.println("id"+idTemp+"删除失败,可能该记录已被删除");
					}
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			rsTemp.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public void setBeginEndId(String beginTime,String endTime) {
		sqlGetBegin = "select min(id) from "+tableName1+" where date >= '"+beginTime+"';";
		sqlGetEnd = "select max(id) from "+tableName1+" where date <= '"+endTime+"';";
		System.out.println(sqlGetBegin);
		System.out.println(sqlGetEnd);
		rs1 = db.executeQuerySQL(sqlGetBegin);
		try {
			if(rs1.next()){
				int a=rs1.getInt(1);
				if(rs1.wasNull()){
					System.out.print("未找到指定时间之后的记录!");
				}else{
				rs2=db.executeQuerySQL(sqlGetEnd);
				if(rs2.next()){	
					int b=rs2.getInt(1);
					if(rs2.wasNull()){
						System.out.print("未找到指定时间之前的记录!");
					}else{
					id_begin=a;
					id_end=b;
					init=true;
					System.out.println("起始id"+id_begin+"，终止id"+id_end);
					}
				}
				}
				
			}else{
				System.out.println("未找到指定时间之后的记录");
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
//		rs2 = db.executeQuerySQL(sqlGetEnd);
//		try {
//			if(rs1.next()&&rs2.next()) {
//				id_begin = rs1.getInt(1);
//				id_end = rs2.getInt(1);
//				
//			}else {
//				lBLog.warn("该重跑时间段内MySQL并未发生变化");
//				return;
//			}
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			lBLog.error(e.getMessage());
//		}
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
		if(args.length!=2){
			System.out.print("参数错误");
			return;
		}else{
			String str1=args[0];
			String str2=args[1];
			ReRun re=new ReRun();
	        try {
				re.startReRun(str1, str2);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("error code 2");
			}finally{
				re.stopReRun();
			}
		}
        
        
	}

}
