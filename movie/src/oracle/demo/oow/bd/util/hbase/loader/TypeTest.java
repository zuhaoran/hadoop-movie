package oracle.demo.oow.bd.util.hbase.loader;

import oracle.demo.oow.bd.pojo.RatingType;
import oracle.demo.oow.bd.to.ActivityTO;

import org.junit.Test;

public class TypeTest
{
	    public static void out()
	    {
	    	RatingType  ratingType = null;
	    	ActivityTO activityTO=new ActivityTO();
	    	int rating=0;
			activityTO.setRating(ratingType.getType(2));
			System.out.println(activityTO.getRating().toString());
	    }
	    public static void main (String args[])
	    {
	    	out();
	    }
}
