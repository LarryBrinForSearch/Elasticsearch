package connectES;

import java.sql.ResultSet;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;

import dbOperations.DBConnect;
import dbOperations.MysqlTools;
import dbOperations.NewsOperate;
import dbOperations.SqlCreator;

public class ElasticSearchInsertTools {
	static NewsOperate news;
    public static boolean insertNewsById(int id,Client client,DBConnect dbcon){
    	boolean success=false;
		 //DBConnect dbcon=new DBConnect("jdbc:mysql://localhost/crawler", "com.mysql.jdbc.Driver", "root", "Mysql1995*");
        //dbcon.connect();
        ResultSet rs=dbcon.executeQuerySQL(SqlCreator.createQueryById(id));
        
        news=new NewsOperate();
        news=MysqlTools.getNewsByRs(rs);
			if(news.inited==true){
				//System.out.println(news.getContent());
			}
			//dbcon.close();
			JsonBuild jsonb=new JsonBuild();
					
			IndexResponse response = client.prepareIndex("lbsearch", "website")
			        .setSource(jsonb.buildNewsJson(news)).setId(String.valueOf(id))
			        .get();
            if(id==Integer.valueOf(response.getId())){
            	success=true;
            }else{
            	System.out.println("Insert false");
            }
			return success;
    }
    
    public static boolean insertNewsByObject(NewsOperate news,Client client,DBConnect dbcon){
    	boolean success=false;
    	if(news.inited==true){
			//System.out.println(news.getContent());
		}
		//dbcon.close();
		JsonBuild jsonb=new JsonBuild();
		
		IndexResponse response = client.prepareIndex("lbsearch", "website")
		        .setSource(jsonb.buildNewsJson(news)).setId(String.valueOf(news.getId()))
		        .get();
		
		if(news.getId()==Integer.valueOf(response.getId())){
        	success=true;
        }else{
        	System.out.println("Insert false");
        }
		return success;
    	
    }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		  String url = "jdbc:mysql://117.78.37.224:3306/crawler";
		    String name = "com.mysql.jdbc.Driver";
		    String user = "root";
		    String password = "Mysql1995*";
		ElasticSearchConnectTools esC=new ElasticSearchConnectTools();
		 esC.connectToES();
		 DBConnect dbcon=new DBConnect(url,name,user,password);
		 dbcon.connect();
		 boolean flag=insertNewsById(25,esC.getClient(),dbcon);
		 if(flag==true){
			 System.out.print("success");
		 }else{
			 System.out.print("fail");
		 }
	}

}
