package oracle.demo.oow.bd.util.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseDB
{
	private Connection conn;
	private static class HbaseDBInstance {
		private static final HBaseDB instance=new HBaseDB();
	}
	public static HBaseDB getInstance() {
		return HbaseDBInstance.instance;
	}
	private HBaseDB() {
		//获取配置类对象
		Configuration conf = HBaseConfiguration.create();
		//指定zookeeper地址
		conf.set("hbase.zookeeper.quorum", "hadoop");
		//指定hbase存储根目录
		conf.set("hbase.rootdir", "hdfs://hadoop:9000/hbase");
		try {
			//获取hbase数据库链接
			conn = ConnectionFactory.createConnection(conf);
			System.out.println("链接成功");
		} catch (IOException e)
		{
			System.out.println("链接失败");
			e.printStackTrace();
		}
	}
	/**
	 * 根据表名称和列族创建表
	 * @param tableName
	 * @param columnFamilies
	 */
	public void createTable(String tableName, String[] columnFamilies,int maxVersions) {
		deleteTable(tableName);
		try {
			Admin admin = conn.getAdmin();
			//指定表名称
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			//添加列族
			for (String string : columnFamilies) {
				HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes(string));
				//family.setMaxVersions(maxVersions);
				descriptor.addFamily(family);
			}
			admin.createTable(descriptor);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据表名称删除表
	 * @param tableName
	 */
	public void deleteTable(String tableName) {
		try {
			Admin admin = conn.getAdmin();
			if(admin.tableExists(TableName.valueOf(tableName))) {
				//首先disabled
				admin.disableTable(TableName.valueOf(tableName));
				//drop
				admin.deleteTable(TableName.valueOf(tableName));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据表名称获取table对象
	 * @param tableName
	 * @return
	 */
	public Table getTable(String tableName) {
		try {
			return conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Long getId(String tableName,String family,String qualifier)
	{
		Long id=null;
		Table table=getTable(tableName);
		try
		{
			id=table.incrementColumnValue(Bytes.toBytes(ConstantsHBase.ROW_KEY_GID_ACTIVITY_ID), Bytes.toBytes(family),Bytes.toBytes(qualifier), 1);
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}
	
	
	//插入数据
	public void put(String tableName,Integer rowkey,String family,String qualifier,String value)
	{
		Table table=getTable(tableName);
		Put put =new Put(Bytes.toBytes(rowkey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try
		{
			table.put(put);
			table.close();
		} catch (Exception e)
		{
			// TODO: handle exception
		}
	}
	
	//插入数据
		public void put(String tableName,Integer rowkey,String family,String qualifier,int value)
		{
			Table table=getTable(tableName);
			Put put =new Put(Bytes.toBytes(rowkey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			try
			{
				table.put(put);
				table.close();
			} catch (Exception e)
			{
				// TODO: handle exception
			}
		}

		//插入数据
			public void put(String tableName,String rowkey,String family,String qualifier,int value)
			{
				Table table=getTable(tableName);
				Put put =new Put(Bytes.toBytes(rowkey));
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
				try
				{
					table.put(put);
					table.close();
				} catch (Exception e)
				{
					// TODO: handle exception
				}
			}		
			//插入数据
			public void put(String tableName,String rowkey,String family,String qualifier,String value)
			{
				Table table=getTable(tableName);
				Put put =new Put(Bytes.toBytes(rowkey));
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
				try
				{
					table.put(put);
					table.close();
				} catch (Exception e)
				{
					// TODO: handle exception
				}
			}	
			public void put(String tableName,Integer rowkey,String family,String qualifier,Double value)
			{
				Table table=getTable(tableName);
				Put put =new Put(Bytes.toBytes(rowkey));
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
				try
				{
					table.put(put);
					table.close();
				} catch (Exception e)
				{
					// TODO: handle exception
				}
			}	
//**********************************************************************************************************
			public Result get(String tableName,String rowkey,String family,String[] Columns) throws IOException
			{
					Table table = conn.getTable(TableName.valueOf(tableName));
					Get get = new Get(Bytes.toBytes(rowkey));
					//获取版本的数量
					//get.setMaxVersions(5);
					
					if(Columns!=null)
					{
						for (String string : Columns)
						{
							get.addColumn(Bytes.toBytes(family), Bytes.toBytes(string));
						}
					}
					Result result = table.get(get);
					table.close();
					return result;

			}
		public Result get(String tableName,int rowkey,String family,String[] Columns) throws IOException
		{
				Table table = conn.getTable(TableName.valueOf(tableName));
				Get get = new Get(Bytes.toBytes(rowkey));
				//获取版本的数量
				//get.setMaxVersions(5);
					
				if(Columns!=null)
				{
					for (String string : Columns)
					{
						get.addColumn(Bytes.toBytes(family), Bytes.toBytes(string));
					}
				}
				Result result = table.get(get);
				table.close();
				return result;

		}
		public void CheckAndDelete(String tableName,Integer rowkey,String family,String qualifier,String value) {
			try {
				Table table = conn.getTable(TableName.valueOf(tableName));
				Delete delete = new Delete(Bytes.toBytes(rowkey));
				
				table.checkAndDelete(Bytes.toBytes(rowkey), Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value), delete);
				
				table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		public void Delete(String tableName,Integer rowkey,String family,String qualifier) {
			try {
				Table table = conn.getTable(TableName.valueOf(tableName));
				Delete delete = new Delete(Bytes.toBytes(rowkey));
				delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
				table.delete(delete);
				table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		public void DeleteNone(String tableName,Integer rowkey,String family,String qualifier) {
			try {
				DeleteNone(tableName, rowkey, family, qualifier);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
//**********************************************************************************************************
		public ResultScanner scan(String tableName,Filter filter)
		{
			ResultScanner resultScanner=null;
			try
			{
				Table table = conn.getTable(TableName.valueOf(tableName));
				Scan scan = new Scan();
				scan.setFilter(filter);
			    resultScanner = table.getScanner(scan);
				for (Result result : resultScanner) {
					System.out.println(result);
				}
				resultScanner.close();
				table.close();

			} catch (Exception e)
			{
				// TODO: handle exception
			}
			return resultScanner;
		}
}
