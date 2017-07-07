package searchFromES;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.Terms;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filters.Filters.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.highlight.HighlightField;
import org.joda.time.DateTime;

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;


/*
 * 查询类
 * 1.简单查询：输入查询词，在title和content中进行搜索，并返回结果：title,content,url,命中数，优先显示title
 * 2.范围内查询：给定范围，输入查询词进行查询。
 * 3.复杂查询：输入多个查询词，以空格分隔进行查询
 * 4.查询结果分析：分析结果来源的文章类型
 */

public class SearchFromES {
	  public static void queryWithString(String target,Client client){
	    	SearchRequestBuilder responsebuilder = client.prepareSearch("crawler").setTypes("website");
	    	SearchResponse myresponse=responsebuilder.setQuery(QueryBuilders.matchPhraseQuery("title", target))  
	    			.setFrom(0).setSize(10).setExplain(true).execute().actionGet();  
	    	SearchHits hits = myresponse.getHits();  
	    	for (int i = 0; i < hits.getHits().length; i++) {  
	    	           //System.out.println(hits.getHits()[i].getSourceAsString());
	    	           System.out.println(hits.getHits()[i].getSource().get("title"));
	    	           }  
	    }
	  /*
	   * 多字段查询：在标题和正文中若出现目标字符串都将会匹配
	   */
	  public static void multiQuery(String target,Client client){
		  SearchRequestBuilder responsebuilder = client.prepareSearch("crawler").setTypes("website");
	    	SearchResponse myresponse=responsebuilder.setQuery(QueryBuilders.multiMatchQuery(target,"title","content"))  
	    			.setFrom(1).setSize(10).setExplain(true).execute().actionGet();  
	    	SearchHits hits = myresponse.getHits(); 
	    	for (int i = 0; i < hits.getHits().length; i++) {  
	    	           //System.out.println(hits.getHits()[i].getSourceAsString());
	    	           System.out.println(hits.getHits()[i].getSource().get("title"));
	    	}  
	    	System.out.println(hits.totalHits()/10+"页");
	    	System.out.println(hits.getHits().length);
	  }
	  
	  /*
	   * 模糊查询
	   */
	  public static void fuzzyQuery(String target,Client client){
		  SearchRequestBuilder responsebuilder = client.prepareSearch("crawler").setTypes("website");
	    	SearchResponse myresponse=responsebuilder.setQuery(QueryBuilders.fuzzyQuery("title",target))  
	    			.setFrom(1).setSize(10).setExplain(true).execute().actionGet();  
	    	SearchHits hits = myresponse.getHits(); 
	    	for (int i = 0; i < hits.getHits().length; i++) {  
	    	           //System.out.println(hits.getHits()[i].getSourceAsString());
	    	           System.out.println(hits.getHits()[i].getSource().get("title"));
	    	}  
	    	System.out.println(hits.totalHits()/10+"页");
	    	System.out.println(hits.getHits().length);
	  }
	 
	  /*
	   * 聚合查询
	   */
//	  public void aggsearch(Client client,String target) {    
//	        SearchResponse response = null;  
//	  
//	        SearchRequestBuilder responsebuilder = client.prepareSearch("crawler")  
//	                .setTypes("website").setFrom(0).setSize(250);  
//	        //聚合的初始化
//	        AggregationBuilder aggregation = AggregationBuilders  
//	                						.terms("agg")  
//	                						.field("channel_name")  
//	                						.subAggregation(  
//	                								AggregationBuilders.topHits("top").setFrom(0)  
//	                								.setSize(10)).size(100);  
//	        
//	        response = responsebuilder.setQuery(QueryBuilders.boolQuery()  
//	        		.should(QueryBuilders.matchPhraseQuery("title", target))
//	        		.should(QueryBuilders.matchPhraseQuery("content", target))
//	        		.minimumShouldMatch("1"))  
//	                //.addSort("category_id", SortOrder.ASC)  
//	                .addAggregation(aggregation)// .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)  
//	                .setExplain(true).execute().actionGet();  
//	  
//	        SearchHits hits = response.getHits();  
//	  
//	        Terms agg = response.getAggregations().get("agg");
//	        
//	        System.out.println(agg.getBuckets().size());  
//	        for (Terms.Bucket entry : agg.getBuckets()) {  
//	            String key = (String) entry.getKey(); // bucket key  
//	            long docCount = entry.getDocCount(); // Doc count  
//	            System.out.println("key " + key + " doc_count " + docCount);  
//	  
//	            // We ask for top_hits for each bucket  
//	            TopHits topHits = entry.getAggregations().get("top");  
//	            for (SearchHit hit : topHits.getHits().getHits()) {  
//	                System.out.println(" -> id " + hit.getId() + " _source [{}]"  
//	                        + hit.getSource().get("category_name"));  
//	                ;  
//	            }  
//	        }  
//	        System.out.println(hits.getTotalHits());  
//	        int temp = 0;  
//	        for (int i = 0; i < hits.getHits().length; i++) {  
//	            // System.out.println(hits.getHits()[i].getSourceAsString());  
//	            System.out.print(hits.getHits()[i].getSource().get("product_id"));  
//	            // if(orderfield!=null&&(!orderfield.isEmpty()))  
//	            // System.out.print("\t"+hits.getHits()[i].getSource().get(orderfield));  
//	            System.out.print("\t"  
//	                    + hits.getHits()[i].getSource().get("category_id"));  
//	            System.out.print("\t"  
//	                    + hits.getHits()[i].getSource().get("category_name"));  
//	            System.out.println("\t"  
//	                    + hits.getHits()[i].getSource().get("name"));  
//	        }  
//	    }  
//	
	  /*
	   * 此方法为聚合查询
	   */
	    
	    public static void aggSearch(Client client){
	    	
	    	SearchRequestBuilder sbuilder = client.prepareSearch("lbsearch").setTypes("website");
	    	
	    	TermsBuilder teamAgg= AggregationBuilders.terms("count").field("channel_name").size(100);
	    	sbuilder.addAggregation(teamAgg);
	    	SearchResponse response = sbuilder.execute().actionGet();
	    	
	    	//aggregation结果解析
	    	java.util.Map<String, Aggregation> aggMap = response.getAggregations().asMap();

	    	System.out.println(aggMap.get("count").toString());
	    	
	    	StringTerms Aggteam= (StringTerms) aggMap.get("count");
	    	Iterator<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> teamBucketIt = Aggteam.getBuckets().iterator();
	    	
	    	int sum=0;
	    
	    	while (teamBucketIt .hasNext()) {
		    	org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket buck = teamBucketIt .next();
		    	//文章类型名
		    	String article = (String) buck.getKey();
		    	//记录数
		    	long counts = buck.getDocCount();
	//	    	//得到所有子聚合
	//	    	Map subaggmap = buck.getAggregations().asMap();
	//	    	//avg值获取方法
	//	    	double avg_age= ((InternalAvg) subaggmap.get("avg_age")).getValue();
	//	    	//sum值获取方法
	//	    	double total_salary = ((InternalSum) subaggmap.get("total_salary")).getValue();
	//	    	//...
	//	    	//max/min以此类推
		    	System.out.println("文章类型："+article+"\t"+"文章数量："+counts);
		    	sum+=counts;
	    	}
	    	System.out.println(sum+"!");
	    }
	    
	    
	    /*
	     * 此方法为先搜索后
	     * client:连接es的对象
	     * target:待查询的关键字
	     * channel_name：查询条件：在某个频道下查询
	     */
	    public static void aggSearch2(Client client,String target,String channel_name){
	    	
	    	//查询条件
	    	String scondition=channel_name==null? "":channel_name;
	    	SearchRequestBuilder sbuilder = client.prepareSearch("lbsearch").setTypes("website");
	    	
	    	TermsBuilder teamAgg= AggregationBuilders.terms("count").field("channel_name").size(20);
	    	sbuilder.addAggregation(teamAgg);
	    	SearchResponse response=null;
	    	if(!"".equals(scondition)){
	    		response = sbuilder.setQuery(QueryBuilders.boolQuery() 
	    				.must(QueryBuilders.matchPhraseQuery("channel_name", scondition))
		        		.should(QueryBuilders.matchPhraseQuery("title", target))
		        		.should(QueryBuilders.matchPhraseQuery("content", target))
		        		.minimumShouldMatch("1"))
	    				.setSize(20)
	    				.addHighlightedField("title")
	    				.addHighlightedField("content")
	    				.setHighlighterPreTags("<span style=\"color:red\">")
	    				.setHighlighterPostTags("</span>")
	    				.setHighlighterPreTags("<em>")
	    				.setHighlighterPostTags("</em>")
		                //.addSort("category_id", SortOrder.ASC)   
		                .setExplain(true).execute().actionGet();  
	    	}
	    	else{
	    		response = sbuilder.setQuery(QueryBuilders.boolQuery() 
		        		.should(QueryBuilders.matchPhraseQuery("title", target))
		        		.should(QueryBuilders.matchPhraseQuery("content", target))
		        		.minimumShouldMatch("1"))
	    				.setSize(20)
		                //.addSort("category_id", SortOrder.ASC)   
		                .setExplain(true).execute().actionGet();  
	    	}
	    	//aggregation结果解析
	    	java.util.Map<String, Aggregation> aggMap = response.getAggregations().asMap();

	    	System.out.println(aggMap.get("count").toString());
	    	
	    	StringTerms Aggteam= (StringTerms) aggMap.get("count");
	    	Iterator<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> teamBucketIt = Aggteam.getBuckets().iterator();
	    	
	    	int sum=0;
	    
	    	while (teamBucketIt .hasNext()) {
		    	org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket buck = teamBucketIt .next();
		    	//文章类型名
		    	String article = (String) buck.getKey();
		    	//记录数
		    	long counts = buck.getDocCount();
	//	    	//得到所有子聚合
	//	    	Map subaggmap = buck.getAggregations().asMap();
	//	    	//avg值获取方法
	//	    	double avg_age= ((InternalAvg) subaggmap.get("avg_age")).getValue();
	//	    	//sum值获取方法
	//	    	double total_salary = ((InternalSum) subaggmap.get("total_salary")).getValue();
	//	    	//...
	//	    	//max/min以此类推
		    	System.out.println("文章类型："+article+"\t"+"文章数量："+counts);
		    	sum+=counts;
	    	}
	    	System.out.println("**************************");
	    	SearchHits hits = response.getHits(); 
	    	for (int i = 0; i < hits.getHits().length; i++) {  
	    	           //System.out.println(hits.getHits()[i].getSourceAsString());
	    	           System.out.println(hits.getHits()[i].getSource().get("title"));
	    	}  
	    	System.out.println(sum+"!");
	    }
	    
	    /*
	     * 高亮显示
	     */
 public static void aggSearch3(Client client,String target,String channel_name){
	    	
	    	//查询条件
	    	String scondition=channel_name==null? "":channel_name;
	    	SearchRequestBuilder sbuilder = client.prepareSearch("lbsearch").setTypes("website");
	    	
	    	TermsBuilder teamAgg= AggregationBuilders.terms("count").field("channel_name").size(20);
	    	sbuilder.addAggregation(teamAgg);
	    	SearchResponse response=null;
	    	if(!"".equals(scondition)){
	    		response = sbuilder.setQuery(QueryBuilders.boolQuery() 
	    				.must(QueryBuilders.matchPhraseQuery("channel_name", scondition))
		        		.should(QueryBuilders.matchPhraseQuery("title", target))
		        		.should(QueryBuilders.matchPhraseQuery("content", target))
		        		.minimumShouldMatch("1"))
	    				.setSize(20)
	    				.addHighlightedField("title")
	    				.addHighlightedField("content")
	    				.setHighlighterPreTags("<span style=\"color:red\">")
	    				.setHighlighterPostTags("</span>")
	    				.setHighlighterPreTags("<em>")
	    				.setHighlighterPostTags("</em>")
		                //.addSort("category_id", SortOrder.ASC)   
		                .setExplain(true).execute().actionGet();  
	    	}
	    	else{
	    		response = sbuilder.setQuery(QueryBuilders.boolQuery() 
		        		.should(QueryBuilders.matchPhraseQuery("title", target))
		        		.should(QueryBuilders.matchPhraseQuery("content", target))
		        		.minimumShouldMatch("1"))
	    				.setSize(20)
	    				.addHighlightedField("title")
	    				.addHighlightedField("content")
	    				.setHighlighterPreTags("<span style=\"color:red\">")
	    				.setHighlighterPostTags("</span>")
	    				
		                //.addSort("category_id", SortOrder.ASC)   
		                .setExplain(true).execute().actionGet();  
	    	}
	    	//aggregation结果解析
	    	java.util.Map<String, Aggregation> aggMap = response.getAggregations().asMap();

	    	System.out.println(aggMap.get("count").toString());
	    	
	    	StringTerms Aggteam= (StringTerms) aggMap.get("count");
	    	Iterator<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> teamBucketIt = Aggteam.getBuckets().iterator();
	    	
	    	int sum=0;
	    
	    	while (teamBucketIt .hasNext()) {
		    	org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket buck = teamBucketIt .next();
		    	//文章类型名
		    	String article = (String) buck.getKey();
		    	//记录数
		    	long counts = buck.getDocCount();
	//	    	//得到所有子聚合
	//	    	Map subaggmap = buck.getAggregations().asMap();
	//	    	//avg值获取方法
	//	    	double avg_age= ((InternalAvg) subaggmap.get("avg_age")).getValue();
	//	    	//sum值获取方法
	//	    	double total_salary = ((InternalSum) subaggmap.get("total_salary")).getValue();
	//	    	//...
	//	    	//max/min以此类推
		    	System.out.println("文章类型："+article+"\t"+"文章数量："+counts);
		    	sum+=counts;
	    	}
	    	System.out.println("**************************");
	    	SearchHits hits = response.getHits();
	    	SearchHit[] shits = hits.getHits();
	        
	    	for (int i = 0; i < hits.getHits().length; i++) {  
	    	           //System.out.println(hits.getHits()[i].getSourceAsString());
	    	      //     System.out.println(hits.getHits()[i].getSource().get("title"));
	    		 SearchHit hit = shits[i];
	    		 Map<String, HighlightField> result = hit.highlightFields();
	    	            //System.out.println(result.size());
	    	           HighlightField titleField = result.get("title");
	    	          
	    	           if (titleField !=null) {
	    	        	   
	                       // 取得定义的高亮标签
	                       Text[] titleTexts = titleField.fragments();
	                       // 为title串值增加自定义的高亮标签
	                       String title = "";
	                       for (Text text : titleTexts) {
	                           title += text;
	                       }
	                     System.out.println(title); //newsInfo.setTitle();
	                   }
	    	           HighlightField contentField = result.get("content");
		    	          
	    	           if (contentField !=null) {
	    	        	   
	                       // 取得定义的高亮标签
	                       Text[] contentTexts = contentField.fragments();
	                       // 为title串值增加自定义的高亮标签
	                       String content = "";
	                       for (Text text : contentTexts) {
	                    	   content += text;
	                       }
	                     System.out.println(content); //newsInfo.setTitle();
	                   }
	    	}  
	    	System.out.println(sum+"!");
	    }
}
