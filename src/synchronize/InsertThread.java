package synchronize;

import java.util.concurrent.Semaphore;

import org.elasticsearch.client.Client;

import dbOperations.DBConnect;

public class InsertThread extends Thread{
    private int min=0;
    private int max=0;
	private boolean inited=false;
	DBConnect dbcon=null;
	Client client=null;
	Semaphore semaphore;
	public void init(int min,int max,DBConnect dbcon,Client client,Semaphore semaphore){
		if(min>0&&max>0&&max>=min){
			this.min=min;
			this.max=max;
			this.dbcon=dbcon;
			this.client=client;
			this.semaphore=semaphore;
			this.inited=true;
		}
	}
	
	public void run(){
		if(inited){
			System.out.println("------start insert from"+min+" to "+max+"Thread"+Thread.currentThread().getName()+"start------");
			if(ReRunUtil.multInsert(min, max, dbcon, client, semaphore)){
				System.out.println("&&&insert from "+min+" to "+max+" success!Thread"+Thread.currentThread().getName()+"will exit");
			}
		}else{
			System.out.println("inited error !Thread "+Thread.currentThread().getName()+"will exit!");
			//Thread.currentThread().destroy();
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
