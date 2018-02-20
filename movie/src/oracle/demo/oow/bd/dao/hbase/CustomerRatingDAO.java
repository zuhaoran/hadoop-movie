package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.pojo.RatingType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.demo.oow.bd.util.mysql.DBBean;
import oracle.demo.oow.bd.pojo.ActivityType;

/**
 * This class is used to access recommended movie data for customer
 */
public class CustomerRatingDAO{
    

    public void insertCustomerRating(int userId, int movieId, int rating) throws IOException {
      HBaseDB db =HBaseDB.getInstance();
      Table table =db.getTable(ConstantsHBase.TABLE_ACTIVITY);
      ActivityDAO activityDAO=new ActivityDAO();
      Date now = new Date(); 
     //进行插入
          Long id=db.getId(ConstantsHBase.TABLE_GID,ConstantsHBase.FAMILY_GID_GID,ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID);
          List<Put> puts =new ArrayList();
          Put put =new Put(Bytes.toBytes(id));
          put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), Bytes.toBytes(movieId));
          put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID), Bytes.toBytes(userId));
          put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY), Bytes.toBytes(ActivityType.RATE_MOVIE.getValue()));
          put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_RATING), Bytes.toBytes(rating));
          put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_PRICE), Bytes.toBytes(0.0));
          put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_TIME), Bytes.toBytes(now.toString()));
          put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED), Bytes.toBytes("y"));
          put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_POSITION), Bytes.toBytes(0));
     	 
          try
		{
			table.put(put);
		      table.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
      
      
    }
    public  ActivityTO	getRating(int userId,int movieId)
    {
    	ActivityTO activityTO=new ActivityTO();
    	HBaseDB db=HBaseDB.getInstance();
    	Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
    	Scan scan = new Scan();

    	// 设置过滤器
    	FilterList filterList=new FilterList();
    	
    	Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), 
    			Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
    			CompareFilter.CompareOp.EQUAL,Bytes.toBytes(userId));
    	((SingleColumnValueFilter) filter).setFilterIfMissing(true);
    	Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), 
    			Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID),
    			CompareFilter.CompareOp.EQUAL,Bytes.toBytes(movieId));
    	((SingleColumnValueFilter) filter2).setFilterIfMissing(true);
    	filterList.addFilter(filter);
    	filterList.addFilter(filter2);
    	scan.setFilter(filterList);
    	RatingType ratingType = null;
    	try
		{
    		ResultScanner resultScanner =table.getScanner(scan);
			if(resultScanner!=null)
			{
				Iterator<Result> iterator=resultScanner.iterator();
				while(iterator.hasNext())
				{
					Result result=iterator.next();
					if(result!=null)
					{
						int rating=Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING)));
						activityTO.setRating(ratingType.getType(rating));
						
						activityTO.setCustId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID))));
						activityTO.setMovieId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))));
					    
						//System.out.println(activityTO.getCustId());
					}
				}
			}
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return activityTO;
    }
    public void deleteCustomerRating(int userId) {
    	HBaseDB db=HBaseDB.getInstance();
    	ActivityDAO activityDAO=new ActivityDAO();
    	List<Integer> activityIds=activityDAO.getActivityIdsByUserId(userId);
    	for (Integer integer : activityIds)
		{
			db.Delete(ConstantsHBase.TABLE_ACTIVITY, integer, ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY, ConstantsHBase.QUALIFIER_ACTIVITY_RATING);
		}
    }

    public List<MovieTO> getMoviesByMood(int userId) {
    	ArrayList<MovieTO> movieList =new ArrayList<MovieTO>();
        ResultSet rs = null;
        MovieTO movieTO = null;
        MovieDAO movieDAO = new MovieDAO();
        ActivityDAO activityDAO=new ActivityDAO();
        List<Integer> movieIds=new ArrayList<Integer>();
        //movieIds=activityDAO.getMovieIdByUserId(userId);
        DBBean db=new DBBean();
        String sql="select movieId from recommend where userId="+userId+" ORDER BY score DESC  limit 100";
        ResultSet resultSet=db.executeQuery(sql);
        try
		{
			while(resultSet.next())
			{
				System.out.println(resultSet.getInt(1));
				movieIds.add(resultSet.getInt(1));
			}
			db.close();
		} catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        for (Integer movieId : movieIds)
		{
			movieTO=movieDAO.getMovieById(movieId);
			try
			{
				movieList.add(movieTO);
			} catch (Exception e)
			{
				System.out.println("添加电影出错");
				// TODO: handle exception
			}
			
		}
        return movieList;
    }

    public static void main(String[] args) {
        List<MovieTO> movieList = new ArrayList<MovieTO>();
        CustomerRatingDAO dao = new CustomerRatingDAO();
        movieList = dao.getMoviesByMood(10);
        if (movieList != null) {
            for (MovieTO movieTO : movieList) {
                System.out.println(movieTO.getMovieJsonTxt());
            } //EOF for
        }
    }


}
