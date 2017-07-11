package connectES;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import dbOperations.NewsOperate;

public class JsonBuild {

	XContentBuilder json =null;
	boolean inited=false;
	
	public  XContentBuilder buildNewsJson(NewsOperate news){
		
		try {
			json=jsonBuilder().startObject()
					.field("id",news.getId())
					.field("tid",news.getTid())
					.field("website_name",news.getWebsite_name())
					.field("region",news.getRegion())
					.field("country",news.getCountry())
					.field("language",news.getLanguage())
					.field("channel_name",news.getChannel_name())
					.field("status",news.getStatus())
					.field("title",news.getTitle())
					.field("content",news.getContent())
					.field("pubtime",news.getPubtime())
					.field("author",news.getAuthor())
					.field("source",news.getSource())
					.field("crawler_time",news.getCrawler_time())
					.field("url",news.getUrl())
					.field("update_time",news.getUpdate_time())
					.endObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("json build error");
			inited=false;
			e.printStackTrace();
		}
		inited=true;		
		return json;
	}
	
	public String getString(){
		String sjson=null;
		if(this.inited==true){
		try {
			sjson= json.string();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sjson;
		}else{
			return null;
		}
	}
}
