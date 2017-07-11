package InsertUpdateDemo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.json.JSONObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;


public class Operations {
	/**
	 * 插入数据到Elasticsearch——总方法——使用json
	 * @param index		索引
	 * @param type		类型
	 * @param idName	Id字段名称
	 * @param json		存储的JSON，可以接受Map
	 * @return
	 */
	public static  Map save(String index, String type, String idName,JSONObject json) {
		List list = new ArrayList();
		list.add(json);
		return save(index, type, idName, list);
	}
	
	
	/**
	 * 插入数据到Elasticsearch——被调用方法——使用javaBean对象集合
	 * @param index		索引
	 * @param type		类型
	 * @param idName	Id字段名称
	 * @param listData  一个对象集合
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static  Map save(String index, String type, String idName,List listData) {
		BulkRequestBuilder bulkRequest = ESTools.client.prepareBulk().setRefresh(true);
		Map resultMap = new HashMap();
		
		
		for (Object object : listData) {
			JSONObject json = JSONObject.fromObject(object);
			//没有指定idName 那就让Elasticsearch自动生成
			if(idName == null){
				IndexRequestBuilder lrb = ESTools.client.prepareIndex(index, type).setSource(json);
				bulkRequest.add(lrb);
			}
			else{
				String idValue = json.optString(idName);
				IndexRequestBuilder lrb = ESTools.client.prepareIndex(index, type,idValue) .setSource(json);
				bulkRequest.add(lrb);	
			}
			
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			// process failures by iterating through each bulk response item
			System.out.println(bulkResponse.getItems().toString());
			resultMap.put("500", "保存ES失败!");
			return resultMap;
		}
		bulkRequest = ESTools.client.prepareBulk();
		resultMap.put("200", "保存ES成功!");
		return resultMap;
	}
}