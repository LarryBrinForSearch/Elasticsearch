package connectES;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;

public class ElasticSearchDeleteTools {
         
	 //ElasticSearchConnectTools esCon=new ElasticSearchConnectTools();
	 
	 public static boolean deleteNewsById(int id,Client client){
//		 while(esCon.isInited()==false){
//			 esCon.connectToES();
//		 }
		 DeleteResponse dResponse = client.prepareDelete(ElasticSearchConnectTools.index, "website", String.valueOf(id)).execute().actionGet();
		 return dResponse.isFound();
	 }
	 
	 public static void main(String args[]){
		 ElasticSearchConnectTools esC=new ElasticSearchConnectTools();
		 esC.connectToES();
		 boolean flag=deleteNewsById(134,esC.getClient());
		 if(flag==true){
			 System.out.print("success");
		 }else{
			 System.out.print("fail");
		 }
	 }
}
