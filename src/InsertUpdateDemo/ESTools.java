package InsertUpdateDemo;

import java.net.InetAddress;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

//import com.sojson.common.utils.LoggerUtils;
//import com.sojson.core.config.IConfig;
public class ESTools {
	
	public final static Client client =  build();
	
	public final static Class clazz = ESTools.class;
	
	
	/**
	 * 创建一次
	 * @return
	 */
	private static Client build(){
		if(null != client){
			return client;
		}
		Client client = null;
		String ip = "127.0.0.1";
		try {
			System.out.println("创建Elasticsearch Client 开始");
			Settings settings = Settings
				.settingsBuilder()
					.put("cluster.name","sojson-application")
						.put("client.transport.sniff", true)
							.build();
			client = TransportClient.builder().settings(settings).build()
			.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), 9300));
			System.out.println("创建Elasticsearch Client 结束");
		} catch (Exception e) {
			System.out.println( "创建Client异常");
		}
		return client;
	}
	
	/**
	 * 关闭
	 */
	public static void close(){
		if(null != client){
			try {
				client.close();
			} catch (Exception e) {
				
			}
		}
	}
	
}