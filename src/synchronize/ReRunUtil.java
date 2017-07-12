package synchronize;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.elasticsearch.client.Client;

import com.sun.rowset.CachedRowSetImpl;

import connectES.ElasticSearchConnectTools;
import connectES.ElasticSearchInsertTools;

import dbOperations.DBConnect;
import dbOperations.MysqlTools;
import dbOperations.NewsOperate;

public class ReRunUtil {

	/**
	 * @param args
	 */
	Semaphore semaphore = new Semaphore(1);
	public static int[] quickSort(int a[]){
		
		return a;
	}
	
	
	public static List<List<Integer>> split(int[] intDataArray) {
        List<List<Integer>> result = new ArrayList<List<Integer>>();
        List<Integer> group = null;
        for (int intValue : intDataArray) {
            if (group == null || group.get(group.size() - 1) + 1 != intValue) {
                group = new ArrayList<Integer>();
                result.add(group);
            }
            group.add(intValue);
        }
        return result;
    }
	
	public static boolean multInsert(int min,int max,DBConnect dbcon,Client client,Semaphore semaphore){
		boolean success=false;
		ElasticSearchConnectTools con=new ElasticSearchConnectTools();
		con.connectToES();
		Client a=con.getClient();
		try {
			          semaphore.acquire();//获取信号灯许可
        } catch (InterruptedException e) {
			       // TODO Auto-generated catch block
			          e.printStackTrace();
	    }
		String sql="select *  from "+ReRun.tableName2+" where  id >="+min+" and id<="+max+";";
		System.out.println("线程"+Thread.currentThread().getName()+"开始执行"+sql);
		ResultSet rs=dbcon.executeQuerySQL(sql);
		try{
		//RowSetFactory factory = RowSetProvider.newFactory();  
		//crs = factory.createCachedRowSet(); 
		CachedRowSetImpl crs = new CachedRowSetImpl();
		crs.populate(rs);
		semaphore.release();
		NewsOperate news=new NewsOperate();
		while(crs.next()){
			news=MysqlTools.getNewsByRs(crs);
			boolean flag=ElasticSearchInsertTools.insertNewsByObject(news, a, dbcon);
			if(flag){
				System.out.println("id"+news.getId()+"insert success");
			}else{
				System.out.println("id"+news.getId()+"insert fail");
			}
		}
		success=true;
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Get cacherowset error in thread"+Thread.currentThread().getName());
			return false;
		}
		return success;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
