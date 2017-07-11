package dbOperations;

import java.sql.Timestamp;

/**
 * 对应内容表的一个对象，将各个字段作为其属性
 * @author YuTian
 *
 */
public class NewsOperate {
           
	private int id=-1;
	private int tid=-1;
    private String website_name=null;
    private String region=null;
    private String country=null;
    private String language=null;
    private String channel_name=null;
    private String status=null;
    private String title=null;
    private String content=null;
    private String pubtime=null;
    private String author=null;
    private String source=null;
    private Timestamp crawler_time=null;
    private String url=null;
    private Timestamp update_time=null;
    public boolean inited=false;
    
    
    public String getPubtime() {
		return pubtime;
	}
	public void setPubtime(String pubtime) {
		this.pubtime = pubtime;
	}
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public Timestamp getCrawler_time() {
		return crawler_time;
	}
	public void setCrawler_time(Timestamp crawler_time) {
		this.crawler_time = crawler_time;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public Timestamp getUpdate_time() {
		return update_time;
	}
	public void setUpdate_time(Timestamp update_time) {
		this.update_time = update_time;
	}
    
    public boolean isInited() {
		return inited;
	}
	public void setInited(boolean inited) {
		this.inited = inited;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getTid() {
		return tid;
	}
	public void setTid(int tid) {
		this.tid = tid;
	}
	public String getWebsite_name() {
		return website_name;
	}
	public void setWebsite_name(String website_name) {
		this.website_name = website_name;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public String getChannel_name() {
		return channel_name;
	}
	public void setChannel_name(String channel_name) {
		this.channel_name = channel_name;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
    
	    
}
