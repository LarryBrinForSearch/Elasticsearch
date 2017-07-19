package connectES;
import java.sql.ResultSet;
import java.util.concurrent.ExecutionException;

import dbOperations.MysqlTools;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;

import dbOperations.NewsOperate;

import com.mysql.jdbc.Connection;

import dbOperations.DBConnect;
import dbOperations.SqlCreator;

public class ElasticSearchUpdateTools {

//	 public static boolean updateNewsById(int id,Client client){
//         boolean success=false;
//         //本地
////		 DBConnect dbcon=new DBConnect("jdbc:mysql://localhost:3306/lbsearch", "com.mysql.jdbc.Driver", "root", "Mysql1995*");
//		 //服务器：
//		 DBConnect dbcon=new DBConnect("jdbc:mysql://localhost:3306/crwaler","com.mysql.jdbc.Driver","root","Mysql1995*");
//		
//		 
//         dbcon.connect();
//         ResultSet rs=dbcon.executeQuerySQL(SqlCreator.createQueryById(id));
//
//         NewsOperate news=new NewsOperate();
//         news=MysqlTools.getNewsByRs(rs);
//			if(news.inited==true){
//				System.out.println(news.getContent());
//			}
//			dbcon.close();
//			JsonBuild jsonb=new JsonBuild();
//           
//            UpdateRequest updateRequest = new UpdateRequest("lbsearch", "website", String.valueOf(id))
//            .doc( jsonb.buildNewsJson(news));
//            try {
//				UpdateResponse uResp=client.update(updateRequest).get();
//				success=true;
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (ExecutionException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			return success;
//	 }
	 
	 public static boolean updateNewsById(int id,Client client,DBConnect dbcon){
         boolean success=false;
         //本地
		 //DBConnect dbcon=new DBConnect("jdbc:mysql://localhost:3306/lbsearch", "com.mysql.jdbc.Driver", "root", "Mysql1995*");
		 //服务器：
//		 DBConnect dbcon=new DBConnect("jdbc:mysql://localhost:3306/crwaler","com.mysql.jdbc.Driver","root","Mysql1995*");	
		 
         //dbcon.connect();
         ResultSet rs=dbcon.executeQuerySQL(SqlCreator.createQueryById(id));

         NewsOperate news=new NewsOperate();
         news=MysqlTools.getNewsByRs(rs);
			if(news.inited==true){
				System.out.println(news.getContent());
			}
//			dbcon.close();
			JsonBuild jsonb=new JsonBuild();
           
            UpdateRequest updateRequest = new UpdateRequest(ElasticSearchConnectTools.index, "website", String.valueOf(id))
            .doc( jsonb.buildNewsJson(news));
            try {
				UpdateResponse uResp=client.update(updateRequest).get();
				success=true;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return success;
	 }
	 
//	 public static void main(String args[]){
//		 ElasticSearchConnectTools esC=new ElasticSearchConnectTools();
//		 esC.connectToES();
//		 boolean flag=updateNewsById(19,esC.getClient());
//		 if(flag==true){
//			 System.out.print("success");
//		 }else{
//			 System.out.print("fail");
//		 }
//	 }
}
