package searchFromES;

import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

public class SearchFromES {
	  public static void queryWithString(String target,Client client){
	    	SearchRequestBuilder responsebuilder = client.prepareSearch("es_t").setTypes("news");
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
		  SearchRequestBuilder responsebuilder = client.prepareSearch("es_t").setTypes("news");
	    	SearchResponse myresponse=responsebuilder.setQuery(QueryBuilders.multiMatchQuery(target,"title","content"))  
	    			.setFrom(0).setSize(10).setExplain(true).execute().actionGet();  
	    	SearchHits hits = myresponse.getHits();  
	    	for (int i = 0; i < hits.getHits().length; i++) {  
	    	           //System.out.println(hits.getHits()[i].getSourceAsString());
	    	           System.out.println(hits.getHits()[i].getSource().get("title"));
	    	}  
	  }
}
