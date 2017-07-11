package synchronize;

import dbOperations.DBConnect;

public class Test {
//	   static String url = "jdbc:mysql://localhost:3306/lbsearch";//jdbc:mysql://117.78.37.224:3306/crwaler"
	   static String url = "jdbc:mysql://localhost:3306/crawler";//jdbc:mysql://117.78.37.224:3306/crwaler"
	   static String name = "com.mysql.jdbc.Driver";
	   static String user = "root";
	   static String password = "Mysql1995*";//Mysql1995*
	   
//	   String 
	   
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//实时更新
		SynchronizeRealTime demo = new SynchronizeRealTime(url,name,user,password,"moniter");
		demo.startSyn();
		
		
	}

}

