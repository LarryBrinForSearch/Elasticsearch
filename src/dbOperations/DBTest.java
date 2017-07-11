package dbOperations;

/**
 * 数据库操作类DBConnect类的测试模块
 * @author MrSong
 *
 */
public class DBTest {
	   static String url = "jdbc:mysql://127.0.0.1/lbsearch";
	   static String name = "com.mysql.jdbc.Driver";
	   static String user = "root";
	   static String password = "";
//	   static String url = "jdbc:mysql://117.78.37.224:3306/crwaler";//jdbc:mysql://127.0.0.1/lbsearch";
//	   static String name = "com.mysql.jdbc.Driver";
//	   static String user = "root";
//	   static String password = "Mysql1995*";
	   
//	   String 
	   
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DBConnect demo = new DBConnect(url,name,user,password);
		demo.connect();
		demo.close();
	}

}
