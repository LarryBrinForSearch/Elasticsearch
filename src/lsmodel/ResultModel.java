package lsmodel;

import java.util.ArrayList;
import org.json.JSONObject;

public class ResultModel {
	private long totalHit;					//总命中数
	private ArrayList<JSONObject> jsonArr;  //结果json数组
	
	public ResultModel(){
		
	}
	public ResultModel(long th,ArrayList<JSONObject> arr){
		this.totalHit = th;
		this.jsonArr = arr;
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
