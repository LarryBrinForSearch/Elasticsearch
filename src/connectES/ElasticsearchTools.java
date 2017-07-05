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
import org.elasticsearch.search.SearchHits;
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

import searchFromES.SearchFromES;  
public class ElasticsearchTools {

	
	//elasticsearch2.3.4客户端
	static Client client = null;
	/*
	 * 设置集群相关信息
	 */
	static{
		Settings settings = Settings.settingsBuilder()  
                .put("cluster.name", "hw_es_cluster").build();  
		try {  
            //初始化连接客户端  
            client = new TransportClient.Builder().settings(settings).build()  
                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.208",9300)))  
                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.224",9300)))  
                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.196",9300)))
                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.177",9300)));  
        }catch (Exception e){  
            e.printStackTrace();  
        }  
	}
        
        
  
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		System.out.println("开始创建索引");
//		client.admin().indices().prepareCreate("xxk").get();
//		System.out.println("索引创建成功");
		SearchFromES.multiQuery("", client);
	}
	/*** 
     *  获取search请求的结果，并输出打印结果信息 
     * @param search 
     * @throws Exception 
     */  
    public  static void showResult(SearchRequestBuilder search) throws Exception{  
        SearchResponse r = search.get();//得到查询结果  
        for(SearchHit hits:r.getHits()){  
            //只能获取addFields里面添加的字段  
//          System.out.println(hits.getFields().get("userId").getValue());  
            //默认可会source里面获取所需字段  
            System.out.println(hits.getSource().get("actId"));  
            //注意不支持data.subjectName这样的访问方式  
            //System.out.println(hits.getId()+"  "+hits.score()+"  "+data.get("subjectName"));  
            //如果是个嵌套json，需要转成map后，访问其属性  
//          Map data=(Map) hits.getSource().get("data");  
//          System.out.println(hits.getId()+"  "+hits.score()+"  "+data.get("subjectName"));  
  
  
        }  
        long hits=r.getHits().getTotalHits();//读取命中数量  
        System.out.println(hits);  
    }  
    
    /*** 
     * 每一天的select count(distinct(actid)) from talbe group by date 
     */  
    public static void countDistinctByField(){  
  
  
        //构造search请求  
        SearchRequestBuilder search=client.prepareSearch("userlog*").setTypes("logs");  
        search.setQuery(QueryBuilders.queryStringQuery("@timestamp:[ "+new DateTime(2016, 8, 8, 0, 0, 0).getMillis()  
                +" TO "+new DateTime(2016, 8, 15, 0, 0, 0).getMillis()+"}"  
        ));  
        search.setSize(0);  
        //一级分组字段  
        DateHistogramBuilder dateagg = AggregationBuilders.dateHistogram("dateagg");  
        dateagg.field("@timestamp");//聚合时间字段  
//      dateagg.interval(DateHistogramInterval.HOUR);//按小时聚合  
        dateagg.interval(DateHistogramInterval.DAY);//按天聚合  
//      dateagg.format("yyyy-MM-dd HH"); //格式化时间  
        dateagg.format("yyyy-MM-dd"); //格式化时间  
        dateagg.timeZone("Asia/Shanghai");//设置时区，注意如果程序部署在其他国家使用时，使用Joda-Time来动态获取时区 new DateTime().getZone()  
  
        //二级分组字段  
//      TermsBuilder twoAgg = AggregationBuilders.terms("stragg").field("actId");  
        MetricsAggregationBuilder twoAgg = AggregationBuilders.cardinality("stragg").field("actId");  
  
        //组装聚合字段  
        dateagg.subAggregation(twoAgg);  
        //向search请求添加  
        search.addAggregation(dateagg);  
        //获取结果  
        SearchResponse r = search.get();  
        Histogram h = r.getAggregations().get("dateagg");  
        //得到一级聚合结果里面的分桶集合  
        List<Histogram.Bucket> buckets = (List<Histogram.Bucket>) h.getBuckets();  
        //遍历分桶集  
        for(Histogram.Bucket b:buckets){  
            //读取二级聚合数据集引用  
            Aggregations sub = b.getAggregations();  
            //获取二级聚合集合  
            Cardinality agg = sub.get("stragg");  
            //获取去重后的值  
            long value = agg.getValue();  
            //如果设置日期的format的时候，需要使用keyAsString取出，否则获取的是UTC的标准时间  
            System.out.println(b.getKeyAsString() +"  " +b.getDocCount()+" "+value);  
        }  
    }  
  
    /*** 
     * 最新版elasticsearch2.3的query测试，结果会评分 
     * @throws Exception 
     */  
    public static void testQuery() throws Exception{  
        SearchRequestBuilder search=client.prepareSearch("userlog*").setTypes("logs");  
        String subjectName="语文";  
        //注意查询的时候，支持嵌套的json查询，通过点符号访问下层字段，读取结果时不支持这种方式  
        search.setQuery(QueryBuilders.queryStringQuery("+data.subjectName:* -data.subjectName:"+subjectName+"  "));  
        showResult(search);  
    }  
  
    /*** 
     * 最新版的elasticsearch2.3的filterquery测试，结果不会评分 
     * @throws Exception 
     */  
    public static void testFilter() throws Exception{  
        SearchRequestBuilder search=client.prepareSearch("userlog*").setTypes("logs");  
        //第一个参数包含的字段数组，第二个字段排除的字段数组  
//      search.setFetchSource(new String[]{"userId","actId"},null);  
//      search.addFields("actId","userId"); //另一种写法  
        String schoolName="沙河市第三小学";  
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()  
                .must(QueryBuilders.queryStringQuery("*:*"))  
                .filter(QueryBuilders.queryStringQuery("+data.subjectName:* +schoolName:"+schoolName).defaultOperator(Operator.AND));  
        //设置query  
        search.setQuery(boolQuery);  
        //打印结果数据  
        showResult(search);  
    }  
  
    /*** 
     *  两个字段分组测试，在时间的维度上加上任意其他的字段聚合，类似group by field1,field2 
     * @throws Exception 
     */  
    public static void testTwoAggString() throws Exception{  
        //构造search请求  
        SearchRequestBuilder search=client.prepareSearch("userlog*").setTypes("logs");  
        search.setQuery(QueryBuilders.queryStringQuery("@timestamp:[ "+new DateTime(2016, 8, 10, 0, 0, 0).getMillis()  
        +" TO "+new DateTime(2016, 8, 11, 0, 0, 0).getMillis()+"}"  
        ));  
        //一级分组字段  
        DateHistogramBuilder dateagg = AggregationBuilders.dateHistogram("dateagg");  
        dateagg.field("@timestamp");//聚合时间字段  
        dateagg.interval(DateHistogramInterval.HOUR);//按小时聚合  
        dateagg.format("yyyy-MM-dd HH"); //格式化时间  
        dateagg.timeZone("Asia/Shanghai");//设置时区，注意如果程序部署在其他国家使用时，使用Joda-Time来动态获取时区 new DateTime().getZone()  
  
        //二级分组字段  
        TermsBuilder twoAgg = AggregationBuilders.terms("stragg").field("module");  
        //组装聚合字段  
        dateagg.subAggregation(twoAgg);  
        //向search请求添加  
        search.addAggregation(dateagg);  
        //获取结果  
        SearchResponse r = search.get();  
        Histogram h = r.getAggregations().get("dateagg");  
        //得到一级聚合结果里面的分桶集合  
        List<Histogram.Bucket> buckets = (List<Histogram.Bucket>) h.getBuckets();  
        //遍历分桶集  
        for(Histogram.Bucket b:buckets){  
            //读取二级聚合数据集引用  
            Aggregations sub = b.getAggregations();  
            //获取二级聚合集合  
            StringTerms count = sub.get("stragg");  
            //如果设置日期的format的时候，需要使用keyAsString取出，否则获取的是UTC的标准时间  
            System.out.println(b.getKeyAsString() +"  " +b.getDocCount());  
            System.out.println("=============================================");  
            for(Terms.Bucket bket:(List<Terms.Bucket>)count.getBuckets()){  
  
                System.out.println(bket.getKeyAsString() +"  "+bket.getDocCount());  
            }  
            System.out.println("************************************************");  
  
        }  
  
    }  
    /*** 
     *  一个字段聚合，类似数据库的group by field1 
     * @param field 测试聚合的字段 
     * @throws Exception 
     */  
    public static  void testOneAggString(String field)throws Exception{  
        //构造search请求  
        SearchRequestBuilder search=client.prepareSearch("userlog*").setTypes("logs");  
        //查询昨天的数据  
        search.setQuery(QueryBuilders.queryStringQuery("@timestamp:[ "+new DateTime(2016, 8, 10, 0, 0, 0).getMillis()  
                +" TO "+new DateTime(2016, 8, 11, 0, 0, 0).getMillis()+"}"  
        ));  
        //聚合构造  
        TermsBuilder termsBuilder = AggregationBuilders.terms("agg").field(field);  
        //添加到search请求  
        search.addAggregation(termsBuilder);  
        //获取结果  
        SearchResponse searchResponse = search.get();  
        //获取agg标识下面的结果  
        Terms agg1 = searchResponse.getAggregations().get("agg");  
        //获取bucket  
        List<Terms.Bucket> buckets = (List<Terms.Bucket>) agg1.getBuckets();  
        long sum=0;  
        for(Terms.Bucket b:buckets){  
            Aggregations sub = b.getAggregations();  
            System.out.println(b.getKeyAsString()+"  "+b.getDocCount());  
            sum+=b.getDocCount();  
        }  
        System.out.println("总数："+sum);  
    }  
  
  

}
