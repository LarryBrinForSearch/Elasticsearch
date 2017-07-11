package dbOperations;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;


public class MysqlTools {
	/*
	 * 获取数据库连接
	 */
//	public static Connection getConn() {
//	    String driver = "com.mysql.jdbc.Driver";//驱动名称
//	    String url = "jdbc:mysql://192.168.56.130:3306/newstest";//主机ip及数据库名称
//	    String username = "root";//数据库用户名
//	    Scanner scan=new Scanner(System.in);
//	    String password = scan.next();//数据库密码
//	    Connection conn = null;
//	    try {
//	        Class.forName(driver); //classLoader,加载对应驱动
//	        conn = (Connection) DriverManager.getConnection(url, username, password);//尝试连接到数据库
//	    } catch (ClassNotFoundException e) {
//	        e.printStackTrace();
//	    } catch (SQLException e) {
//	        e.printStackTrace();
//	    }
//	    return conn;
//	}
//	
//	/*
//	 * 通过id获取对应数据
//	 */
//	private static int getById(int id,Connection connect) {
//	    Connection conn = connect;//获取连接
//	    String sql = "select * from news where id='"+id+"';";//查询语句
//	    PreparedStatement pstmt;
//	    try {
//	        pstmt = (PreparedStatement)conn.prepareStatement(sql);
//	        ResultSet rs = pstmt.executeQuery();
//	        int col = rs.getMetaData().getColumnCount();
//	        System.out.println("==========="+col+"=================");
//	        while (rs.next()) {
//	            for (int i = 1; i <= col; i++) {
//	                System.out.print(rs.getString(i) + "\n");
////	                if ((i == 2) && (rs.getString(i).length() < 8)) {
////	                    System.out.print("\t");
////	                }
//	             }
//	            System.out.println("");
//	        }
//	            System.out.println("============================");
//	    } catch (SQLException e) {
//	        e.printStackTrace();
//	        return -1;
//	    }
//	    return 0;
//	}
//	
//	public static NewsOperate getNewsById(int id,Connection connect) {
//	    Connection conn = connect;//获取连接
//	    String sql = "select * from news where id='"+id+"';";//查询语句
//	    PreparedStatement pstmt;
//	    NewsOperate news=new NewsOperate();
//	    try {
//	        pstmt = (PreparedStatement)conn.prepareStatement(sql);
//	        ResultSet rs = pstmt.executeQuery();
//	        int col = rs.getMetaData().getColumnCount();
//	        if(16==col){
//	        	if(rs.next()){
//	        	news.setId(Integer.parseInt(rs.getString(1)));
//	        	news.setTid(Integer.parseInt(rs.getString(2)));
//	        	news.setWebsite_name(rs.getString(3));
//	        	news.setRegion(rs.getString(4));
//	        	news.setCountry(rs.getString(5));
//	        	news.setLanguage(rs.getString(6));
//	        	news.setChannel_name(rs.getString(7));
//	        	news.setStatus(rs.getString(8));
//	        	news.setTitle(rs.getString(9));
//	        	news.setContent(rs.getString(10));
//	        	news.setPubtime(rs.getString(11));
//	        	news.setAuthor(rs.getString(12));
//	        	news.setSource(rs.getString(13));
//	        	news.setCrawler_time(rs.getTimestamp(14));
//	        	news.setUrl(rs.getString(15));
//	        	news.setUpdate_time(rs.getTimestamp(16));
//	        	news.setInited(true);
//	        	}
//	        }else{
//	        	news.setInited(false);
//	        }
//	            
//	    } catch (SQLException e) {
//	        e.printStackTrace();
//	    }
//	    return news;
//	}
//	
	public static NewsOperate getNewsByRs(ResultSet rs){
		NewsOperate news=new NewsOperate();
		 int col;
		try {
			col = rs.getMetaData().getColumnCount();
			if(16==col){
	        	if(rs.next()){
	        	news.setId(Integer.parseInt(rs.getString(1)));
	        	news.setTid(Integer.parseInt(rs.getString(2)));
	        	news.setWebsite_name(rs.getString(3));
	        	news.setRegion(rs.getString(4));
	        	news.setCountry(rs.getString(5));
	        	news.setLanguage(rs.getString(6));
	        	news.setChannel_name(rs.getString(7));
	        	news.setStatus(rs.getString(8));
	        	news.setTitle(rs.getString(9));
	        	news.setContent(rs.getString(10));
	        	news.setPubtime(rs.getString(11));
	        	news.setAuthor(rs.getString(12));
	        	news.setSource(rs.getString(13));
	        	news.setCrawler_time(rs.getTimestamp(14));
	        	news.setUrl(rs.getString(15));
	        	news.setUpdate_time(rs.getTimestamp(16));
	        	news.setInited(true);
	        	}
	        }else{
	        	news.setInited(false);
	        }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			news.setInited(false);
			e.printStackTrace();	
		}	 
		 return news;
	}
	
//	public static void main(String args[]){
//		Connection con=getConn();
//		int flag=getById(14,con);
//		if(flag!=0)
//			System.out.println("SQL execute fail");
//		
//		NewsOperate news=new NewsOperate();
//		news=getNewsById(14,con);
//		if(news.inited==true){
//			System.out.println(news.getContent());
//		}
//		
//	}

}
