package oracle.demo.oow.bd.util.hbase;

/**
 * 创建对应的hbase表结构
 * @author Administrator
 *
 */
public class InitTable {

	public static void main(String[] args) {
		
		HBaseDB db = HBaseDB.getInstance();
		String tableName = "user";
		String[] columnFamilies = {"info", "id"};
		db.createTable(tableName, columnFamilies,1);
		
	}
	
}
