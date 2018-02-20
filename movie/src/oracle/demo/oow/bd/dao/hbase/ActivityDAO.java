package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.constant.KeyConstant;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.pojo.BooleanType;
import oracle.demo.oow.bd.pojo.RatingType;

import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.MovieTO;

import oracle.demo.oow.bd.util.FileWriterUtil;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;



import org.apache.avro.Schema;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import org.codehaus.jackson.node.ObjectNode;

public class ActivityDAO {

    public ActivityDAO() {
       
    }

    public void insertCustomerActivity(ActivityTO activityTO) {
        int custId = 0;
        int movieId = 0;
        ActivityType activityType = null;
        String jsonTxt = null;
        if (activityTO != null) {
            jsonTxt = activityTO.getJsonTxt();
            System.out.println("User Activity| " + jsonTxt);
            /**
             * This system out should write the content to the application log
             * file.
             */
            FileWriterUtil.writeOnFile(activityTO.getActivityJsonOriginal().toString());
            
            custId = activityTO.getCustId();
            movieId = activityTO.getMovieId();

            if (custId > 0 && movieId > 0) {
                activityType = activityTO.getActivity();
                
                HBaseDB db=HBaseDB.getInstance();
                Table table=db.getTable(ConstantsHBase.TABLE_ACTIVITY);
                Long id=db.getId(ConstantsHBase.TABLE_GID,ConstantsHBase.FAMILY_GID_GID,ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID);
                Put put =new Put(Bytes.toBytes(id));
                try
				{
                	put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), Bytes.toBytes(activityTO.getMovieId()));
                    
				} catch (Exception e)
				{
					// TODO: handle exception
				}
                try
				{
                	 put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY), Bytes.toBytes(activityTO.getActivity().getValue()));
                     
				} catch (Exception e)
				{
					// TODO: handle exception
				}
                try
				{
                	 put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID), Bytes.toBytes(activityTO.getGenreId()));
                     
				} catch (Exception e)
				{
					// TODO: handle exception
				}
                try
				{
                	 put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_POSITION), Bytes.toBytes(activityTO.getPosition()));
                     
				} catch (Exception e)
				{
					// TODO: handle exception
				}
                
                try
				{
                	 put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_PRICE), Bytes.toBytes(activityTO.getPrice()));
                     
				} catch (Exception e)
				{
					// TODO: handle exception
				} try
				{
					  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_RATING), Bytes.toBytes(activityTO.getRating().getValue()));
		              
				} catch (Exception e)
				{
					// TODO: handle exception
				}
				 try
				{
					  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED), Bytes.toBytes(activityTO.isRecommended().getValue()));
		               	
				} catch (Exception e)
				{
						// TODO: handle exception
				}
				 try
				{
					 put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_TIME), Bytes.toBytes(activityTO.getTimeStamp()));
		              
				} catch (Exception e)
					{
						// TODO: handle exception
				}
				 try
				{
					 put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes( ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID), Bytes.toBytes(activityTO.getCustId()));
		               
				} catch (Exception e)
					{
						// TODO: handle exception
					}
				 
                 
                try
				{
					table.put(put);
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
               
                }
            } //if (custId > 0 && movieId > 0)

        } //if (activityTO != null)

  
    public List<Integer> getMovieIdByUserId(int custId)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	ArrayList<Integer> movieIds =new ArrayList<Integer>();
    	Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
					
		Filter filter = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(custId));
    	((SingleColumnValueFilter) filter).setFilterIfMissing(true);
    	
    	scan.setFilter(filter);
		ResultScanner resultScanner = null;
		try
		{
			resultScanner = table.getScanner(scan);
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (Result result : resultScanner) {
			movieIds.add(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))));
        	
		}
    	
    	
    	return movieIds;
    }
    public List<Integer> getActivityIdsByUserId(int custId)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	ArrayList<Integer> activityIds =new ArrayList<Integer>();
    	Scan scan=new Scan();
    	Filter filter = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(custId));
    	((SingleColumnValueFilter) filter).setFilterIfMissing(true);
    	ResultScanner results= db.scan(ConstantsHBase.TABLE_ACTIVITY, filter);
    	
    	for (Result result : results)
		{
    		activityIds.add(Bytes.toInt(result.getRow()));
    	    
		}
    	return activityIds;
    }

    public List<Integer> getMovieIdByUserId(int custId,int activity)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	ArrayList<Integer> movieIds =new ArrayList<Integer>();
    	Scan scan=new Scan();
    	FilterList filterList=new FilterList();
    	Filter filter = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(custId));
    	((SingleColumnValueFilter) filter).setFilterIfMissing(true);
    	Filter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(activity));
    	((SingleColumnValueFilter) filter).setFilterIfMissing(true);
    	filterList.addFilter(filter);
    	filterList.addFilter(filter2);
    	ResultScanner results= db.scan(ConstantsHBase.TABLE_ACTIVITY, filterList);
    	
    	for (Result result : results)
		{
    	    movieIds.add(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))));
    	    
		}
    	return movieIds;
    }
    public List<Integer> getMovieIdAll()
    {
    	HBaseDB db=HBaseDB.getInstance();
    	ArrayList<Integer> movieIds =new ArrayList<Integer>();
    	Scan scan=new Scan();
    	Filter filter = new FamilyFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY)));
    	ResultScanner results= db.scan(ConstantsHBase.TABLE_ACTIVITY, filter);
    	for (Result result : results)
		{
    	    movieIds.add(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))));
    	    
		}
    	return movieIds;
    }
    
    public boolean isExistByUserIdAndMovieId(int userId,int movieId)
    {
    	Boolean b=false;
    	ActivityTO activityTO=null;
    	activityTO=getActivityTO(userId, movieId);
    	if(activityTO==null)
    	{
    		b=false;
    	}
    	else
    	{
    		b=true;
    	}
    	return b;
    }
    public  int getIdByUserIdAndMovieId(int userId,int movieId)
    {
    	int id=-1;
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
						id=Bytes.toInt(result.getRow());
					}
				}
			}
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return id;
    }
    public  ActivityTO	getActivityTO(int userId,int movieId)
    {
    	ActivityTO activityTO=null;
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
    	/*Filter filter3 = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID)));
*/
    	((SingleColumnValueFilter) filter2).setFilterIfMissing(true);
    	filterList.addFilter(filter);
    	filterList.addFilter(filter2);
    /*	filterList.addFilter(filter3);*/
    	scan.setFilter(filterList);

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
						activityTO=new ActivityTO();
						try
						{
							int rating=Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING)));
							RatingType ratingType = null;
							activityTO.setRating(ratingType.getType(rating));
						} catch (Exception e)
						{
							// TODO: handle exception
						}
						try
						{
							activityTO.setCustId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID))));
							
						} catch (Exception e)
						{
							// TODO: handle exception
						}
						try
						{
							activityTO.setMovieId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))));
							
						} catch (Exception e)
						{
							// TODO: handle exception
						}
						try
						{
							activityTO.setGenreId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID))));
							
						} catch (Exception e)
						{
							// TODO: handle exception
						}
						try
						{
							String recommended=Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED)));
							BooleanType booleanType = null;
							activityTO.setRecommended(booleanType.getType(recommended));
						} catch (Exception e)
						{
							// TODO: handle exception
						}
						try
						{
							activityTO.setTimeStamp(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME))));
							
						} catch (Exception e)
						{
							// TODO: handle exception
						}
						try
						{
							activityTO.setPrice(Bytes.toDouble(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE))));
							
						} catch (Exception e)
						{
							// TODO: handle exception
						}
						try
						{
							activityTO.setPosition(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION))));
							
						} catch (Exception e)
						{
							// TODO: handle exception
						}
						try
						{
							int activity=Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID)));
							ActivityType activityType=null;
							activityTO.setActivity(activityType.getType(activity)); 
						} catch (Exception e)
						{
							// TODO: handle exception
						}
						
						
						/*try
						{
							activityTO.setGenreId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID))));
							String recommended=Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED)));
							activityTO.setTimeStamp(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME))));
							activityTO.setPrice(Bytes.toDouble(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE))));
							activityTO.setPosition(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION))));
							int activity=Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID)));
							ActivityType activityType=null;
							activityTO.setActivity(activityType.getType(activity)); 
							BooleanType booleanType = null;
							activityTO.setRecommended(booleanType.getType(recommended));
						} catch (Exception e)
						{
							// TODO: handle exception
						}*/
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
    
    public List<Integer> qvchong(List<Integer> movieIds)
    {
    	List<Integer> newMovieIds=new ArrayList<Integer>();
    	Iterator<Integer> iterator= movieIds.iterator();
    	int last=-1;
    	while(iterator.hasNext())
    	{
    		if(last==iterator.next())
    		{
    			continue;
    		}
    		else
    		{
    			last=iterator.next();
    			newMovieIds.add(last);
    		}
    	}
    	return newMovieIds;
    }
    //????????????????????????????????????????????????????????
    public List<MovieTO> getCustomerBrowseList(int userId)
    {
    	//通过用户id获得浏览的movieid 列表
    	MovieDAO movieDAO=new MovieDAO();
    	List<Integer> movieIds=qvchong(getMovieIdByUserId(userId));
        List<MovieTO> movieList =movieDAO.getMoviesByKey(movieIds);
        return movieList;
    }
    
    public List<MovieTO> getCommonPlayList() {
    	//通过用户id获得观看的movieid 列表
    	MovieDAO movieDAO=new MovieDAO();
    	List<Integer> movieIds=qvchong(getMovieIdAll());
        List<MovieTO> movieList = movieDAO.getMoviesByKey(movieIds);
        return movieList;    }
    
    //?????????????????????????????????????
    public List<MovieTO> getCustomerHistoricWatchList(int custId) {
       
    	//通过用户id获得观看的movieid 列表
    	MovieDAO movieDAO=new MovieDAO();
    	List<Integer> movieIds=qvchong(getMovieIdByUserId(custId));
        List<MovieTO> movieList = movieDAO.getMoviesByKey(movieIds);
        return movieList;
    }

    //????????????????????????????????????????
    //通过用户id 获取观看列表（movieid）
    public List<MovieTO> getCustomerCurrentWatchList(int custId) {
    	//通过用户id获得观看的movieid 列表askajs
    	MovieDAO movieDAO=new MovieDAO();
    	List<Integer> movieIds=qvchong(getMovieIdByUserId(custId));
        List<MovieTO> movieList = movieDAO.getMoviesByKey(movieIds);
        return movieList;
    }
}

   
