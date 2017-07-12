package connectES;
import java.net.InetSocketAddress;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;  
import org.elasticsearch.action.search.SearchResponse;  
import org.elasticsearch.client.Client;  
import org.elasticsearch.client.transport.TransportClient;  
import org.elasticsearch.common.settings.Settings;  
import org.elasticsearch.common.transport.InetSocketTransportAddress;  
import org.elasticsearch.index.query.BoolQueryBuilder;  
import org.elasticsearch.index.query.QueryBuilders;  
import org.elasticsearch.index.query.QueryStringQueryBuilder.Operator;  
import org.elasticsearch.search.SearchHit;  
import org.elasticsearch.search.aggregations.AggregationBuilders;  
import org.elasticsearch.search.aggregations.Aggregations;  
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;  
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;  
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;  
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;  
import org.elasticsearch.search.aggregations.bucket.terms.Terms;  
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.joda.time.DateTime;

import lsmodel.ResultModel;
import searchFromES.SearchFromES;  
public class ElasticsearchTools {

	
	//elasticsearch2.3.4客户端
	static Client client = null;
	/*
	 * 设置集群相关信息
	 */
//	static{
//		Settings settings = Settings.settingsBuilder()  
//                .put("cluster.name", "hw_es_cluster").build();  
//		try {  
//            //初始化连接客户端  
//            client = new TransportClient.Builder().settings(settings).build()  
//                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.208",9300)))  
//                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.224",9300)))  
//                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.196",9300)))
//                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.177",9300)));  
//        }catch (Exception e){  
//            e.printStackTrace();  
//        }  
//	}
        
        
 
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		System.out.println("开始创建索引");
//		client.admin().indices().prepareCreate("xxk").get();
//		System.out.println("索引创建成功");
		SearchFromES.multiQuery("中国美国", null, 15);
		SearchFromES.testIk("中国美国新加坡");
		//System.out.println(rm.getJsonArr());
		System.out.println("end");
	}
	

}
