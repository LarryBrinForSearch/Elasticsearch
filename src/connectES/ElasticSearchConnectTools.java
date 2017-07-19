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
public class ElasticSearchConnectTools {
    public static String index="larrybrin";
	private Client client = null;
	boolean inited=false;
	
	public Client getClient() {
		return client;
	}
	public void setClient(Client client) {
		this.client = client;
	}
	public boolean isInited() {
		return inited;
	}
	public void setInited(boolean inited) {
		this.inited = inited;
	}
	
	public void connectToES(){
		Settings settings = Settings.settingsBuilder()  
                .put("cluster.name", "hw_es_cluster").build();
//        .put("cluster.name", "elasticsearch").build();
		
		try {  
            //初始化连接客户端  
            client = new TransportClient.Builder().settings(settings).build()  
//                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.208",9300))) 
                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.224",9300))) ; 
//                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.196",9300)))
//                  .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.177",9300)));  
//                  .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("127.0.0.1",9300)));  
        }catch (Exception e){  
            e.printStackTrace();  
        }  
		this.setInited(true);
	}
	
}
