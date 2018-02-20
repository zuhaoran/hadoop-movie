package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;


import oracle.demo.oow.bd.dao.hbase.ActivityDAO;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
public class UserDao{    
	private static Table customerTable = null;
	public final static String TABLE_NAME="CUSTOMER";
	public final static String CHILD_TABLE="customerGenres";
	public final static String CUSTOMER_GENRE_MOVIE_TABLE = "customerGenreMovie";

	private static int MOVIE_MAX_COUNT = 25;
	private static int GENRE_MAX_COUNT = 10;

	private static final String PASSWORD = StringUtil.getMessageDigest("welcome1");
	private static final String USERNAME = "guest";
	//修改user中的info和id列族
	public void insert(CustomerTO customerTO) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_USER);
		if(table!=null) {
			Put put = new Put(Bytes.toBytes(customerTO.getUserName()));
			//username --> id的映射
			put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_ID), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_ID), Bytes.toBytes(customerTO.getId()));
			//用户的基本信息
			Put put2 = new Put(Bytes.toBytes(customerTO.getId()));
			put2.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_USER), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_NAME), Bytes.toBytes(customerTO.getName()));
			put2.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_USER), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_EMAIL), Bytes.toBytes(customerTO.getEmail()));
			put2.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_USER), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_USERNAME), Bytes.toBytes(customerTO.getUserName()));
			put2.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_USER_USER), Bytes.toBytes(ConstantsHBase.QUALIFIER_USER_PASSWORD), Bytes.toBytes(customerTO.getPassword()));
			
			List<Put> puts = new ArrayList();
			puts.add(put);
			puts.add(put2);
			try {
				table.put(puts);
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try
		{
			table.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public CustomerTO getCustomerByCredential(String username, String password) {
		CustomerTO customerTO = null;
		
		//首先通过username查询id
		int id = getIdByUserName(username);
		if(id>0) {
			customerTO = getInfoById(id);
			if(customerTO!=null)
			{
				if(!customerTO.getPassword().equals(password))//验证通过
				{
						customerTO=null;
						
				}
			}
		}
		//根据id查询基本信息
		
		return customerTO;
	}

	private CustomerTO getInfoById(int id) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_USER);
		Get get = new Get(Bytes.toBytes(id));
		CustomerTO customerTO = new CustomerTO();
		try {
			Result result = table.get(get);
			customerTO.setEmail(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("email"))));
			customerTO.setName(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			customerTO.setId(id);
			customerTO.setPassword(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
			customerTO.setUserName(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("username"))));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		try
		{
			table.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return customerTO;
	}

	public int getIdByUserName(String username) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable("user");
		Get get = new Get(Bytes.toBytes(username));
		int id = 0;
		try {
			Result result = table.get(get);
			id = Bytes.toInt(result.getValue(Bytes.toBytes("id"), Bytes.toBytes("id")));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		try
		{
			table.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}
	public List<MovieTO> getMovies4CustomerByGenre(int custId, int genreId, int maxCount) {
		 HBaseDB hBaseDB=HBaseDB.getInstance();
			Table table=hBaseDB.getTable(ConstantsHBase.TABLE_GENRE);
			Scan scan=new Scan();
			scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE));
			Filter filter=new PrefixFilter(Bytes.toBytes(genreId+"_"));
			Filter filter2=new PageFilter(maxCount);
			FilterList filterList=new FilterList(filter,filter2);
			scan.setFilter(filterList);
			
			ResultScanner resultScanner=null;
			try
			{
				resultScanner=table.getScanner(scan);
				
			} catch (Exception e)
			{
				// TODO: handle exception
			}
			List<MovieTO> movieTOs=new ArrayList<MovieTO>();
			MovieTO movieTO=null;
			if(resultScanner!=null)
			{
				Iterator<Result> iter=resultScanner.iterator();
				MovieDAO movieDAO=new MovieDAO();
				while(iter.hasNext())
				{
					Result result=iter.next();
					if(result!=null&!result.isEmpty())
					{
						int movieId=Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID)));
						movieTO=movieDAO.getMovieById(movieId);
						
						if(StringUtil.isNotEmpty(movieTO.getPosterPath()))
						{
							movieTO.setOrder(100);
						}
						else
						{
							movieTO.setOrder(0);
						}
						movieTOs.add(movieTO);				
						
					}
				}
				
			}
			try
			{
				table.close();
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return movieTOs;
	 }
	public List<MovieTO> getMovies4CustomerByGenre(int custId, int genreId) {
		HBaseDB hBaseDB=HBaseDB.getInstance();
		Table table=hBaseDB.getTable(ConstantsHBase.TABLE_GENRE);
		Scan scan=new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE));
		Filter filter=new PrefixFilter(Bytes.toBytes(genreId+"_"));
		Filter filter2=new PageFilter(MOVIE_MAX_COUNT);
		FilterList filterList=new FilterList(filter,filter2);
		scan.setFilter(filterList);
		
		ResultScanner resultScanner=null;
		try
		{
			resultScanner=table.getScanner(scan);
			
		} catch (Exception e)
		{
			// TODO: handle exception
		}
		List<MovieTO> movieTOs=new ArrayList<MovieTO>();
		MovieTO movieTO=null;
		if(resultScanner!=null)
		{
			Iterator<Result> iter=resultScanner.iterator();
			MovieDAO movieDAO=new MovieDAO();
			while(iter.hasNext())
			{
				Result result=iter.next();
				if(result!=null&!result.isEmpty())
				{
					int movieId=Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID)));
					movieTO=movieDAO.getMovieById(movieId);
					
					if(StringUtil.isNotEmpty(movieTO.getPosterPath()))
					{
						movieTO.setOrder(100);
					}
					else
					{
						movieTO.setOrder(0);
					}
					movieTOs.add(movieTO);				
					
				}
			}
			
		}
		try
		{
			table.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return movieTOs;
	}
	
	//获取6个分类
	public List<GenreMovieTO> getMovies4Customer(int custId, int movieMaxCount, int genreMaxCount)
	{
		List<GenreMovieTO> genreTOs=new ArrayList<>();
		Scan scan=new Scan();
		
		Filter filter=new PageFilter(genreMaxCount);
		scan.setFilter(filter);
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE));
		HBaseDB db =HBaseDB.getInstance();
		Table table=db.getTable(ConstantsHBase.TABLE_GENRE);
		try
		{
			ResultScanner resultScanner=table.getScanner(scan);
			Iterator<Result> iter=resultScanner.iterator();
			GenreTO genreTO=null;
			while(iter.hasNext())
			{
				genreTO=new GenreTO();
				Result result=iter.next();
				genreTO.setId(Bytes.toInt(result.getRow()));
				genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE),Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME))));
				GenreMovieTO genreMovieTO=new GenreMovieTO();
				genreMovieTO.setGenreTO(genreTO);
				genreTOs.add(genreMovieTO);
			}
		} catch (Exception e)
		{
			// TODO: handle exception
		}
		return genreTOs;
	}
	
	public ActivityTO getMovieRating(int custId, int movieId) {
		CustomerRatingDAO ratingDAO=new CustomerRatingDAO();
		ActivityTO activityTO;
		activityTO=ratingDAO.getRating(custId, movieId);
        return activityTO;
	} 
}
