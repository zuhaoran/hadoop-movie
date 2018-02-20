package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;


import oracle.demo.oow.bd.dao.hbase.MovieDAO;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;



public class CrewDAO{

	 public void insertCrewInfo(CrewTO crewTO)
	    {
	    	HBaseDB db =HBaseDB.getInstance();
	    	db.put(ConstantsHBase.TABLE_CREW, crewTO.getId(),ConstantsHBase.FAMILY_CREW_CREW, ConstantsHBase.QUALIFIER_CREW_NAME, crewTO.getName());
	    	db.put(ConstantsHBase.TABLE_CREW, crewTO.getId(),ConstantsHBase.FAMILY_CREW_CREW, ConstantsHBase.QUALIFIER_CREW_JOB, crewTO.getJob());
	    	insertCrewToMovie(crewTO);
	    }
	    private void insertCrewToMovie(CrewTO crewTO)
	    {
	    	MovieDAO movieDAO=new MovieDAO();
	    	HBaseDB db=HBaseDB.getInstance();
	    	List<String>MovieIds=crewTO.getMovieList();
	    	for (String movieId : MovieIds)
			{
	    		db.put(ConstantsHBase.TABLE_CREW, crewTO.getId()+"_"+movieId,ConstantsHBase.FAMILY_CREW_MOVIE, ConstantsHBase.QUALIFIER_CREW_MOVIE_ID, movieId);
	    		movieDAO.insertMovieCrew(crewTO, movieId);
			}
	    	//List<E>
	    	//db.put(ConstantsHBase.TABLE_CREW, rowkey, family, qualifier, value)
	    }
	    public CrewTO getCrewById(int crewId)
	    {
	    	HBaseDB db=HBaseDB.getInstance();
	    	CrewTO crewTO=new CrewTO();
	    	crewTO.setId(crewId);
	    	try
			{
				Result result= db.get(ConstantsHBase.TABLE_CREW, crewId, ConstantsHBase.FAMILY_CREW_CREW, null);
				if(result!=null)
				{
					crewTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME))));
					crewTO.setJob(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB))));
					
				}
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	List<String> movieList=new ArrayList<String>();
	    	Table table=db.getTable(ConstantsHBase.TABLE_CREW);
	    	Scan scan=new Scan();
	    	scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE));
	    	Filter filter=new PrefixFilter(Bytes.toBytes(crewId+"_"));
	    	scan.setFilter(filter);
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
							movieList.add(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_MOVIE_ID))));
						}
					}
				}
			table.close();
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	crewTO.setMovieList(movieList);
	    	return crewTO;
	    }
	   
	    /**
	     * This method returns a list of Casts for the movieId passed
	     * Key= /MV_CW/movieId/-/crewId
	     * @param movieId - Unique Id of the movie
	     * @return List of CrewTO
	     */
	    public List<CrewTO> getMovieCrews(int movieId) {

	        List<CrewTO> crewList = null;

	        CastCrewTO castCrewTO = null;
	        MovieTO movieTO=null;
	        MovieDAO movieDAO=new MovieDAO();
	        if (movieId > -1) {
	           movieTO=movieDAO.getMovieById(movieId);
	           castCrewTO=movieTO.getCastCrewTO();
	           crewList=castCrewTO.getCrewList();
	          

	        } //if (movieId > -1)


	        return crewList;
	    } //getMovieCrews

	    /**
	     * This method returns all the movies that Crew worked in.
	     * @param crewId
	     * @return List of MovieTO
	     */
	    public List<MovieTO> getMoviesByCrew(int crewId) {
	        List<String> movieIdList = null;
	        List<MovieTO> movieList = new ArrayList<MovieTO>();
	        CrewTO crewTO = null;
	        crewTO=getCrewById(crewId);
	        MovieDAO movieDAO=new MovieDAO();
	        movieIdList=crewTO.getMovieList();
	        Iterator<String> iterator=movieIdList.iterator();
	        while(iterator.hasNext())
	        {
	        	movieList.add(movieDAO.getMovieById(iterator.next()));
	        }
	        return movieList;
	    } //getMoviesByCrew
	 
}//CrewDAO
