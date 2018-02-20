package oracle.demo.oow.bd.util.hbase.loader;

import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.dao.hbase.ActivityDAO;
import oracle.demo.oow.bd.dao.hbase.CustomerRatingDAO;
import oracle.demo.oow.bd.to.MovieTO;

public class RecommendTest
{
	public static void main(String arg[])
	{
		int userId=1255601;
		//CustomerRatingDAO dao=new CustomerRatingDAO();
		//List<MovieTO> list=dao.getMoviesByMood(1255601);
		ActivityDAO activityDAO=new ActivityDAO();
		List<MovieTO> list=activityDAO.getCustomerBrowseList(userId);
		Iterator<MovieTO> iterator=list.iterator();
		while(iterator.hasNext())
		{
			MovieTO movieTO=iterator.next();
			System.out.println(movieTO.getMovieJsonTxt());
		}
	}
}
