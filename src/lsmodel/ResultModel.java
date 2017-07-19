package lsmodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

public class ResultModel {
	private long totalHit;					//总命中数
	private ArrayList<JSONObject> jsonArr;  //结果json数组
	private String time =null;
	private HashMap<String,Long> staMap; //统计数据
	
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public ResultModel(){
		
	}
	public ResultModel(long th,ArrayList<JSONObject> arr,String times,HashMap<String,Long> staMap){
		this.totalHit = th;
		this.jsonArr = arr;
		this.time=times;
		this.staMap=staMap;
	}
	
	public HashMap<String, Long> getStaMap() {
		return staMap;
	}
	public void setStaMap(HashMap<String, Long> staMap) {
		this.staMap = staMap;
	}
	public long getTotalHit() {
		return totalHit;
	}
	public void setTotalHit(long totalHit) {
		this.totalHit = totalHit;
	}
	public ArrayList<JSONObject> getJsonArr() {
		return jsonArr;
	}
	public void setJsonArr(ArrayList<JSONObject> jsonArr) {
		this.jsonArr = jsonArr;
	}
	
}
