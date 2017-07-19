package searchFromES;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.highlight.HighlightField;
import org.json.JSONObject;

import lsmodel.ResultModel;;


/*
 * 查询类
 * 1.简单查询：输入查询词，在title和content中进行搜索，并返回结果：title,content,url,命中数，优先显示title
 * 2.范围内查询：给定范围，输入查询词进行查询。
 * 3.复杂查询：输入多个查询词，以空格分隔进行查询
 * 4.查询结果分析：分析结果来源的文章类型
 */

public class SearchFromES {
	
	static Client client = null;
	
	static{
		Settings settings = Settings.settingsBuilder()  
                .put("cluster.name", "hw_es_cluster").build();  
		try {  
            //初始化连接客户端  
            client = new TransportClient.Builder().settings(settings).build()  
                    //.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.208",9300)))  
                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.224",9300)))  
                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.196",9300)))
                    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("117.78.37.177",9300)));  
        }catch (Exception e){  
            e.printStackTrace();  
        }  
		System.out.println("ES has been connected");
	}
	
	
	public static void queryWithString(String target) {
		SearchRequestBuilder responsebuilder = client.prepareSearch("crawler").setTypes("website");
		SearchResponse myresponse = responsebuilder.setQuery(QueryBuilders.matchPhraseQuery("title", target)).setFrom(0)
				.setSize(10).setExplain(true).execute().actionGet();
		SearchHits hits = myresponse.getHits();
		for (int i = 0; i < hits.getHits().length; i++) {
			// System.out.println(hits.getHits()[i].getSourceAsString());
			System.out.println(hits.getHits()[i].getSource().get("title"));
		}
	}

	/*
	 * match实现的多字段查询：在标题和正文中若出现目标字符串都将会匹配
	 */
	public static void Query(String target) {
		SearchRequestBuilder responsebuilder = client.prepareSearch("crawler").setTypes("website");
		SearchResponse myresponse = responsebuilder.setQuery(
				QueryBuilders.multiMatchQuery(target, "title", "content"))
				.setFrom(200)
				.setSize(10)
				.setExplain(true).execute().actionGet();
		SearchHits hits = myresponse.getHits();
		for (int i = 0; i < hits.getHits().length; i++) {
			// System.out.println(hits.getHits()[i].getSourceAsString());
			System.out.println(hits.getHits()[i].getSource().get("title"));
		}
		System.out.println(hits.totalHits() / 10 + "页");
		System.out.println(hits.getHits().length);
	}

	
	//需要的返回结果有url，title，content，pubtime，channel_name，website_name，score
	public static ResultModel  QueryAtFirst(String target,String channel_name,int pageSize){
		
		//构建搜索请求
		//索引为lbsearch，类型为website
		SearchRequestBuilder responsebuilder = client.prepareSearch("lbsearch").setTypes("website");

		if (channel_name !=null)
		{
			responsebuilder.setQuery(QueryBuilders.boolQuery()
					.must(QueryBuilders.matchPhraseQuery("channel_name", channel_name))
					.should(QueryBuilders.matchPhraseQuery("title", target).boost(3f))			//标题权值高于正文
					.should(QueryBuilders.matchPhraseQuery("content", target).boost(0.8f))		
					.minimumShouldMatch("1"));			//标题和正文至少匹配一次
		}
		else{
			responsebuilder.setQuery(QueryBuilders.boolQuery()
					.should(QueryBuilders.matchPhraseQuery("title", target).boost(3f))			//标题权值高于正文
					.should(QueryBuilders.matchPhraseQuery("content", target).boost(0.8f))		
					.minimumShouldMatch("1"));
		}
		responsebuilder .setFrom(1)							//从第几条开始
						.setSize(pageSize)						//显示几条
						.addHighlightedField("title") 							// 高亮显示的域
						.addHighlightedField("content")
						.setHighlighterPreTags("<span style=\"color:red\">") 	// 设置高亮域前置标签
						.setHighlighterPostTags("</span>") 						// 设置高亮域后置标签
						.setExplain(true);
		
		SearchResponse myresponse = responsebuilder.execute().actionGet();						//进行查询
		//得到命中的结果
		SearchHits hits = myresponse.getHits();
		
		ArrayList<JSONObject> jsonArr=new ArrayList<JSONObject>();
		
		for (int i = 0; i < hits.getHits().length; i++) {
			
			SearchHit hit = hits.getHits()[i];
			// 得到命中条目的高亮显示域
			Map<String, HighlightField> result = hit.highlightFields();

			HighlightField titleField = result.get("title");
			HighlightField contentField = result.get("content");
			
			String title = titleField==null? (String)hit.getSource().get("title") : "";
			String content = contentField==null? (String)hit.getSource().get("content") : "";
			
			if (titleField != null) {
				// 取得定义的高亮标签
				Text[] titleTexts = titleField.fragments();
				// 为title串值增加自定义的高亮标签
				for (Text text : titleTexts) {
					title += text;
				}
				System.out.println(hit.getScore()+"\t"+hit.getId()+"\t"+title); // 已經被高亮的title
			}
			if (contentField != null) {
				// 取得定义的高亮标签
				Text[] contentTexts = contentField.fragments();
				// 为title串值增加自定义的高亮标签
				for (Text text : contentTexts) {
					content += text;
				}
				//System.out.println(hit.getScore()+"\t"+hit.getId());  // 已經被高亮的content
			}
			
			Map params= hits.getHits()[i].getSource();								//得到查询结果的数据源
			Map<String,String> par = new HashMap<String,String>();
			par.put("url", (String)params.get("url"));
			par.put("title", title);			//取title的前70个字符
			par.put("content", content);	//取content的前300个字符
			par.put("pubtime", (String)params.get("pubtime"));						//得到发布时间：
			par.put("channel_name", (String)params.get("channel_name"));			//频道名称
			par.put("website_name", (String)params.get("website_name"));			//网站来源名称
			par.put("score", hits.getHits()[i].getScore()+"");						//匹配度
	
			JSONObject array = new JSONObject(par);
			jsonArr.add(array);
		}
		System.out.println(hits.totalHits() / 10 + "页");
		System.out.println(hits.getHits().length);
		System.out.println(myresponse.getTook());
		
		return new ResultModel(myresponse.getHits().getTotalHits(),jsonArr,myresponse.getTook()+"",null);
	}
	
	
	//需要的返回结果有url，title，content，pubtime，channel_name，website_name，score
		public static ResultModel  QueryLater(int atPages,int pageSize,String target,String channel_name){
			
			//构建搜索请求
			//索引为lbsearch，类型为website
			SearchRequestBuilder responsebuilder = client.prepareSearch("lbsearch").setTypes("website");
			
			if (channel_name !=null)
			{
				responsebuilder.setQuery(QueryBuilders.boolQuery()
						.must(QueryBuilders.commonTermsQuery("channel_name", channel_name))
						.should(QueryBuilders.commonTermsQuery("title", target).boost(3f))			//标题权值高于正文
						.should(QueryBuilders.commonTermsQuery("content", target).boost(0.8f))		
						.minimumShouldMatch("1"));			//标题和正文至少匹配一次
			}
			else{
				responsebuilder.setQuery(QueryBuilders.boolQuery()
						.should(QueryBuilders.commonTermsQuery("title", target).boost(3f))			//标题权值高于正文
						.should(QueryBuilders.commonTermsQuery("content", target).boost(0.8f))		
						.minimumShouldMatch("1"));
			}
			responsebuilder .setFrom((atPages-1)*pageSize+1)							//从第几条开始
							.setSize(pageSize)						//显示几条
							.addHighlightedField("title") 							// 高亮显示的域
							.addHighlightedField("content")
							.setHighlighterPreTags("<span style=\"color:red\">") 	// 设置高亮域前置标签
							.setHighlighterPostTags("</span>") 						// 设置高亮域后置标签
							.setExplain(true);
			
			SearchResponse myresponse = responsebuilder.execute().actionGet();						//进行查询
			//得到命中的结果
			SearchHits hits = myresponse.getHits();
			
			ArrayList<JSONObject> jsonArr=new ArrayList<JSONObject>();
			
			for (int i = 0; i < hits.getHits().length; i++) {
				
				SearchHit hit = hits.getHits()[i];
				// 得到命中条目的高亮显示域
				Map<String, HighlightField> result = hit.highlightFields();

				HighlightField titleField = result.get("title");
				HighlightField contentField = result.get("content");
				
				String title = titleField==null? (String)hit.getSource().get("title") : "";
				String content = contentField==null? (String)hit.getSource().get("content") : "";
				
				if (titleField != null) {
					// 取得定义的高亮标签
					Text[] titleTexts = titleField.fragments();
					// 为title串值增加自定义的高亮标签
					for (Text text : titleTexts) {
						title += text;
					}
					//System.out.println(hit.getScore()+"\t"+hit.getId()); // 已經被高亮的title
				}
				if (contentField != null) {
					// 取得定义的高亮标签
					Text[] contentTexts = contentField.fragments();
					// 为title串值增加自定义的高亮标签
					for (Text text : contentTexts) {
						content += text;
					}
					//System.out.println(hit.getScore()+"\t"+hit.getId());  // 已經被高亮的content
				}
				
				Map params= hits.getHits()[i].getSource();								//得到查询结果的数据源
				Map<String,String> par = new HashMap<String,String>();
				par.put("url", (String)params.get("url"));
				par.put("title", title);			//取title的前70个字符
				par.put("content", content);	//取content的前300个字符
				par.put("pubtime", (String)params.get("pubtime"));						//得到发布时间：
				par.put("channel_name", (String)params.get("channel_name"));			//频道名称
				par.put("website_name", (String)params.get("website_name"));			//网站来源名称
				par.put("score", hits.getHits()[i].getScore()+"");						//匹配度
				
				JSONObject array = new JSONObject(par);
				jsonArr.add(array);
			}
			System.out.println(hits.totalHits() / 15 + "页");
			System.out.println(hits.getHits().length);
			System.out.println(myresponse.getTook());
			
			return new ResultModel(myresponse.getHits().getTotalHits(),jsonArr,myresponse.getTook()+"",null);
		}
		
		
	/*
	 * 模糊查询
	 */
	public static void fuzzyQuery(String target) {
		SearchRequestBuilder responsebuilder = client.prepareSearch("crawler").setTypes("website");
		SearchResponse myresponse = responsebuilder.setQuery(
				QueryBuilders.fuzzyQuery("title", target))
				.setFrom(1)
				.setSize(10)
				.setExplain(true).execute().actionGet();
		SearchHits hits = myresponse.getHits();
		for (int i = 0; i < hits.getHits().length; i++) {
			// System.out.println(hits.getHits()[i].getSourceAsString());
			System.out.println(hits.getHits()[i].getSource().get("title"));
		}
		System.out.println(hits.totalHits() / 10 + "页");
		System.out.println(hits.getHits().length);
	}

	/*
	 * 此方法为聚合查询，查詢的是當前所有文檔 輸出結果按照aggField聚合并排序
	 */

	public static Map aggSearch(String aggField) {

		SearchRequestBuilder sbuilder = client.prepareSearch("larrybrin").setTypes("website");
		
		TermsBuilder teamAgg = AggregationBuilders.terms("count").field(aggField).size(100);
		sbuilder.addAggregation(teamAgg);
		SearchResponse response = sbuilder.execute().actionGet();

		// aggregation结果解析
		java.util.Map<String, Aggregation> aggMap = response.getAggregations().asMap();

		System.out.println(aggMap.get("count").toString());

		Aggregation Aggteam = aggMap.get("count");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> teamBucketIt = ((InternalTerms<StringTerms, Bucket>) Aggteam).getBuckets()
				.iterator();

		Map<String,Integer> result=new HashMap<String,Integer>();
		
		while (teamBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket buck = teamBucketIt.next();
			// 文章类型名
			String article = (String) buck.getKey();
			// 记录数
			long counts = buck.getDocCount();
			if(article.length()!=1)
			System.out.println(article+'\t'+counts);
			result.put(article, (int)counts);
		}
		return result;
	}

	/*
	 * 此方法为先搜索后 client:连接es的对象 target:待查询的关键字 channel_name：查询条件：在某个频道下查询
	 * 此外，還支持高亮显示
	 */
	public static void aggSearch3(String target, String channel_name) {

		// 如果查询频道是null,那么scondition为null
		String scondition = channel_name == null ? "" : channel_name;

		// 构造搜索请求
		SearchRequestBuilder sbuilder = client.prepareSearch("lbsearch").setTypes("website");

		// 构造聚合条件。等价于group by channel_name
		TermsBuilder teamAgg = AggregationBuilders.terms("count").field("channel_name").size(20);
		sbuilder.addAggregation(teamAgg);

		SearchResponse response = null;
		// 如果查询条件不为空，则必须要满足查询条件
		if (!"".equals(scondition)) {
			sbuilder.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery("channel_name", scondition))
					.should(QueryBuilders.matchPhraseQuery("title", target).boost(3f))
					.should(QueryBuilders.matchPhraseQuery("content", target).boost(0.8f))
					.minimumShouldMatch("1"));
		} else {
			sbuilder.setQuery(QueryBuilders.boolQuery()
					.should(QueryBuilders.matchPhraseQuery("title", target).boost(6f))
					.should(QueryBuilders.matchPhraseQuery("content", target).boost(0.5f))
					.minimumShouldMatch("1"));
		}
		response = sbuilder.setSize(100) 								// 显示20条
				.addHighlightedField("title") 							// 高亮显示的域
				.addHighlightedField("content")
				.setHighlighterPreTags("<span style=\"color:red\">") 	// 设置高亮域前置标签
				.setHighlighterPostTags("</span>") 						// 设置高亮域后置标签
				.setExplain(true).execute().actionGet(); 				// 解析执行

		// aggregation结果解析
		java.util.Map<String, Aggregation> aggMap = response.getAggregations().asMap();
        //得到聚合count的项
		StringTerms Aggteam = (StringTerms) aggMap.get("count");
		Iterator<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> teamBucketIt = Aggteam.getBuckets()
				.iterator();

		while (teamBucketIt.hasNext()) {
			org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket buck = teamBucketIt.next();
			// 文章类型名
			String article = (String) buck.getKey();
			// 记录数
			long counts = buck.getDocCount();
			System.out.println("文章类型：" + article + "\t" + "文章数量：" + counts);
		}
		System.out.println("**************************");
		SearchHits hits = response.getHits();
		SearchHit[] shits = hits.getHits();

		for (int i = 0; i < hits.getHits().length; i++) {
			SearchHit hit = shits[i];
			// 得到命中条目的高亮显示域
			Map<String, HighlightField> result = hit.highlightFields();

			HighlightField titleField = result.get("title");
			HighlightField contentField = result.get("content");

			if (titleField != null) {
				// 取得定义的高亮标签
				Text[] titleTexts = titleField.fragments();
				// 为title串值增加自定义的高亮标签
				String title = "";
				for (Text text : titleTexts) {
					title += text;
				}
				System.out.println(hit.getScore()+"\t"+hit.getId()+"\t"+title); // 已經被高亮的title
			}

			if (contentField != null) {
				// 取得定义的高亮标签
				Text[] contentTexts = contentField.fragments();
				// 为title串值增加自定义的高亮标签
				String content = "";
				for (Text text : contentTexts) {
					content += text;
				}
				//System.out.println(hit.getScore()+"\t"+hit.getId());  // 已經被高亮的content
			}
		}
		System.out.println(hits.totalHits());
	}
	
	
	
//	public static void testIk(String s){
//		AnalyzeRequestBuilder ikRequest = new AnalyzeRequestBuilder(client,
//                							AnalyzeAction.INSTANCE,"lbsearch",s);
//        ikRequest.setTokenizer("ik_smart");
//        List<AnalyzeResponse.AnalyzeToken> ikTokenList = ikRequest.execute().actionGet().getTokens();
//
//        // 循环赋值
//        List<String> searchTermList = new ArrayList<>();
//        ikTokenList.forEach(ikToken -> { 
//        	searchTermList.add(ikToken.getTerm()); 
//        	System.out.println(ikToken.getTerm());
//        	});
//	}
	
	//需要的返回结果有url，title，content，pubtime，channel_name，website_name，score
		public static ResultModel  multiQuery(String target,String channel_name,int pageSize){
			
			//构建搜索请求
			//索引为lbsearch，类型为website
			SearchRequestBuilder responsebuilder = client.prepareSearch("lbsearch").setTypes("website");
			
			// 构造聚合条件。等价于group by channel_name
			TermsBuilder teamAgg = AggregationBuilders.terms("count").field("channel_name").size(20);
			responsebuilder.addAggregation(teamAgg);
			if (channel_name !=null)
			{
				responsebuilder.setQuery(QueryBuilders.boolQuery()
						.must(QueryBuilders.commonTermsQuery("channel_name", channel_name))
						.should(QueryBuilders.commonTermsQuery("title", target).boost(3f))			//标题权值高于正文
						.should(QueryBuilders.commonTermsQuery("content", target).boost(0.8f))		
						.minimumShouldMatch("1"));			//标题和正文至少匹配一次
			}
			else{
				responsebuilder.setQuery(QueryBuilders.boolQuery()
						.should(QueryBuilders.commonTermsQuery("title", target).boost(3f))			//标题权值高于正文
						.should(QueryBuilders.commonTermsQuery("content", target).boost(0.8f))		
						.minimumShouldMatch("1"));
			}
			responsebuilder .setFrom(1)							//从第几条开始
							.setSize(pageSize)						//显示几条
							.addHighlightedField("title") 							// 高亮显示的域
							.addHighlightedField("content")
							.setHighlighterPreTags("<span style=\"color:red\">") 	// 设置高亮域前置标签
							.setHighlighterPostTags("</span>") 						// 设置高亮域后置标签
							.setExplain(true);
			
			SearchResponse myresponse = responsebuilder.execute().actionGet();						//进行查询
			//得到命中的结果
			SearchHits hits = myresponse.getHits();
			
			ArrayList<JSONObject> jsonArr=new ArrayList<JSONObject>();
			
			for (int i = 0; i < hits.getHits().length; i++) {
				
				SearchHit hit = hits.getHits()[i];
				// 得到命中条目的高亮显示域
				Map<String, HighlightField> result = hit.highlightFields();

				HighlightField titleField = result.get("title");
				HighlightField contentField = result.get("content");
				
				String title = titleField==null? (String)hit.getSource().get("title") : "";
				String content = contentField==null? (String)hit.getSource().get("content") : "";
				
				if (titleField != null) {
					// 取得定义的高亮标签
					Text[] titleTexts = titleField.fragments();
					// 为title串值增加自定义的高亮标签
					for (Text text : titleTexts) {
						title += text;
					}
					System.out.println(hit.getScore()+"\t"+hit.getId()+"\t"+title); // 已經被高亮的title
				}
				if (contentField != null) {
					// 取得定义的高亮标签
					Text[] contentTexts = contentField.fragments();
					// 为title串值增加自定义的高亮标签
					for (Text text : contentTexts) {
						content += text;
					}
					//System.out.println(hit.getScore()+"\t"+hit.getId());  // 已經被高亮的content
				}
				
				Map params= hits.getHits()[i].getSource();								//得到查询结果的数据源
				Map<String,String> par = new HashMap<String,String>();
				par.put("url", (String)params.get("url"));
				par.put("title", title);			//取title的前70个字符
				par.put("content", content);	//取content的前300个字符
				par.put("pubtime", (String)params.get("pubtime"));						//得到发布时间：
				par.put("channel_name", (String)params.get("channel_name"));			//频道名称
				par.put("website_name", (String)params.get("website_name"));			//网站来源名称
				par.put("score", hits.getHits()[i].getScore()+"");						//匹配度
				
				JSONObject array = new JSONObject(par);
				jsonArr.add(array);
			}
			
			//统计结果解析
			// aggregation结果解析
			java.util.Map<String, Aggregation> aggMap = myresponse.getAggregations().asMap();
	        //得到聚合count的项
			StringTerms Aggteam = (StringTerms) aggMap.get("count");
			Iterator<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> teamBucketIt = Aggteam.getBuckets()
					.iterator();
			//统计结果
			HashMap<String,Long> countResult=new HashMap<String,Long>();
			
			long resultSum = 0;	//统计结果的总数
			long mainResult= 0;	//前六条统计结果总数
			int cs=0; 			//取前六条
			while (teamBucketIt.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket buck = teamBucketIt.next();
				// 记录数
				long counts = buck.getDocCount();
				
				if(cs<6){
					countResult.put((String) buck.getKey(), counts);// 文章类型名，记录数
					mainResult +=counts;
				}
				resultSum += counts;
				cs++;
			}
			countResult.put("其他", resultSum-mainResult);
			return new ResultModel(myresponse.getHits().getTotalHits(),jsonArr,myresponse.getTook()+"",countResult);
		}
	
		public static void prefixQuery(String pre){
			SearchRequestBuilder responsebuilder = client.prepareSearch("larrybrin").setTypes("website");
			SearchResponse myresponse = responsebuilder.setQuery(
					QueryBuilders.prefixQuery("title", pre))
					.setFrom(1)
					.setSize(10)
					.setExplain(false).execute().actionGet();
			SearchHits hits = myresponse.getHits();
			for (int i = 0; i < hits.getHits().length; i++) {
				// System.out.println(hits.getHits()[i].getSourceAsString());
				System.out.println(hits.getHits()[i].getSource().get("title"));
			}
			System.out.println(hits.totalHits() / 10 + "页");
			System.out.println(hits.getHits().length);
		}
		
//		public static void main(String args[]){
//			SearchFromES.aggSearch("title");
//			System.out.println("end");
//		}
		
//		 public static void queryWords(String query) throws IOException {
//		        Configuration cfg = DefaultConfig.getInstance();
//		        System.out.println(cfg.getMainDictionary()); // 系统默认词库
//		        System.out.println(cfg.getQuantifierDicionary());
//		        List<String> list = new ArrayList<String>();
//		        StringReader input = new StringReader(query.trim());
//		        IKSegmenter ikSeg = new IKSegmenter(input, true);   // true 用智能分词 ，false细粒度
//		        for (Lexeme lexeme = ikSeg.next(); lexeme != null; lexeme = ikSeg.next()) {
//		            System.out.print(lexeme.getLexemeText()+"|");
//		        }
//
//		    }
		
}
