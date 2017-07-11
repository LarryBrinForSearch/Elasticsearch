package dbOperations;


public class SqlCreator {
         
	  public static String createQueryById(int id){
		  return "select * from website where id='"+id+"';";
	  }
}
