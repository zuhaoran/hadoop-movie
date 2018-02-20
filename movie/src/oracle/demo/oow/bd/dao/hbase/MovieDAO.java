package oracle.demo.oow.bd.dao.hbase;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.generated.master.table_jsp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.google.protobuf.Descriptors.MethodDescriptor;

import oracle.demo.oow.bd.dao.CastCrewDAO;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;


import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;


public class MovieDAO {

    /**
     * This method inserts Movie information and index its ID as well as name.
     * Movie may belong to many genres so this method also create mapping between
     * genre and movie.
     * @param movieTO - This is a movie transfer object
     * @return - true if insertion is successful
     */
    public boolean insertMovie(MovieTO movieTO) throws Exception{
        boolean flag = true;
        String name = null;

        HBaseDB db=HBaseDB.getInstance();
        Table table=db.getTable(ConstantsHBase.TABLE_MOVIE);
       
        	db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE, ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE, movieTO.getTitle());
        	db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE, ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW, movieTO.getOverview());
        	db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE, ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH, movieTO.getPosterPath());
        	db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE, ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE, movieTO.getDate());
        	db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE, ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT, movieTO.getVoteCount());
        	db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE, ConstantsHBase.QUALIFIER_MOVIE_RUNTIME, movieTO.getRunTime());
        	db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(), ConstantsHBase.FAMILY_MOVIE_MOVIE, ConstantsHBase.QUALIFIER_MOVIE_POPULARITY, movieTO.getPopularity());
            insertMovieGenres(movieTO);
        
        return flag;
    } //insertMovie

    public void insertMovieGenres(MovieTO movieTO)throws Exception
    {
    	HBaseDB db=HBaseDB.getInstance();
    	List<GenreTO> genreTOs=movieTO.getGenres();
    	GenreDAO genreDAO=new GenreDAO();
    	for (GenreTO genreTO : genreTOs)
    	{
    		db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId()+"_"+genreTO.getId(), ConstantsHBase.FAMILY_MOVIE_GENRE, ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID, genreTO.getId());
    		db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId()+"_"+genreTO.getId(), ConstantsHBase.FAMILY_MOVIE_GENRE, ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME, genreTO.getName());
    		if(!genreDAO.isExist(genreTO))
    		{
    			//分类数据
    			genreDAO.insertGenre(genreTO);
    		}
    		
    		genreDAO.insertGenreMovie(movieTO, genreTO);
    	}
    }
    public void insertMovieCast(CastTO castTO,Integer movieid)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	db.put(ConstantsHBase.TABLE_MOVIE, movieid+"_"+castTO.getId(), ConstantsHBase.FAMILY_MOVIE_CAST, ConstantsHBase.QUALIFIER_MOVIE_CAST_ID, castTO.getId());
    }
    public void insertMovieCrew(CrewTO crewTO,String movieid)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	db.put(ConstantsHBase.TABLE_MOVIE, movieid+"_"+crewTO.getId(), ConstantsHBase.FAMILY_MOVIE_CREW, ConstantsHBase.QUALIFIER_MOVIE_CREW_ID, crewTO.getId());
    }
//***********************************************************************************************
   //通过movieid获取
    public List<MovieTO> getMoviesByKey(List<Integer> movieIds) {
        if (movieIds.size()==0)
            return new ArrayList<MovieTO>();
       
        
        List<MovieTO> movieTOList = new ArrayList<MovieTO>();
        MovieTO movieTO=new MovieTO();
        for (int  movieId: movieIds)
		{
        	movieTO=getMovieById(movieId);
        	movieTOList.add(movieTO);
		}
                
        //Sort them based on the order
        Collections.sort(movieTOList);

        return movieTOList;
        } //getMovies4Key
    public MovieTO getMovieById(String movieIdStr) {
        int movieId = 0;
        if (StringUtil.isNotEmpty(movieIdStr)) {
            try {
                movieId = Integer.parseInt(movieIdStr);
            } catch (NumberFormatException ne) {
                movieId = 0;
            } //EOF try/catch


        } //EOF if
        return getMovieById(movieId);
    } //getMovie
    public MovieTO getMovieInfoById(int movieId)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	MovieTO movieTO=new MovieTO();
    	try
		{
			Result result=db.get(ConstantsHBase.TABLE_MOVIE, movieId,ConstantsHBase.FAMILY_MOVIE_MOVIE , null);
			movieTO.setId(movieId);
			try
			{
				movieTO.setTitle(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE))));
			} catch (Exception e)
			{
				// TODO: handle exception
			}
			try
			{
				movieTO.setOverview(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW))));
			} catch (Exception e)
			{
				// TODO: handle exception
			}
			try
			{
				movieTO.setPosterPath(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH))));
			} catch (Exception e)
			{
				// TODO: handle exception
			}
			try
			{
				movieTO.setDate(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE))));
			} catch (Exception e)
			{
				// TODO: handle exception
			}
			
			try
			{
				movieTO.setVoteCount(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT))));
			} catch (Exception e)
			{
				// TODO: handle exception
			}
			
			try
			{
				movieTO.setRunTime(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME))));
			} catch (Exception e)
			{
				// TODO: handle exception
			}
			try
			{
				movieTO.setPopularity(Bytes.toDouble(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY))));
			} catch (Exception e)
			{
				// TODO: handle exception
			}
			
			
			
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return movieTO;
    }
    public MovieTO getMovieById(int movieId)
    {
    	MovieTO movieTO=new MovieTO();
    	movieTO=getMovieDetailById(movieId);
    	return movieTO;
    }
    public ArrayList<GenreTO> getGenresByMovieId(int movieId)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	Table table=db.getTable(ConstantsHBase.TABLE_MOVIE);
    	
    	Scan scan=new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE));
		Filter filter=new PrefixFilter(Bytes.toBytes(movieId+"_"));
		scan.setFilter(filter);
		ResultScanner resultScanner=null;
		try
		{
			resultScanner=table.getScanner(scan);
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList<GenreTO> genreTOs=new ArrayList<GenreTO>();
		GenreTO genreTO=new GenreTO();
		if(resultScanner!=null)
		{
			Iterator<Result> iter=resultScanner.iterator();
			while(iter.hasNext())
			{
				Result result=iter.next();
				if(result!=null&!result.isEmpty())
				{
					genreTO.setId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE),(Bytes.toBytes( ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID)))));
					genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE),(Bytes.toBytes( ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME)))));	
					genreTOs.add(genreTO);
				}
			}
		}
    	return genreTOs;
    }
    public List<CastTO> getCastsByMovieId(int movieId)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	Table table=db.getTable(ConstantsHBase.TABLE_MOVIE);
    	
    	Scan scan=new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST));
		Filter filter=new PrefixFilter(Bytes.toBytes(movieId+"_"));
		scan.setFilter(filter);
		ResultScanner resultScanner=null;
		try
		{
			resultScanner=table.getScanner(scan);
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList<CastTO> castTOs=new ArrayList<CastTO>();
		CastTO castTO=new CastTO();
		CastDAO castDAO =new CastDAO();
		if(resultScanner!=null)
		{
			Iterator<Result> iter=resultScanner.iterator();
			while(iter.hasNext())
			{
				Result result=iter.next();
				if(result!=null&!result.isEmpty())
				{
					castTO.setId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST),(Bytes.toBytes( ConstantsHBase.QUALIFIER_MOVIE_CAST_ID)))));
					
					castTOs.add(castDAO.getCastById(castTO.getId()));
				}
			}
		}
    	return castTOs;
    
    }
    public List<CrewTO> getCrewsByMovieId(int movieId)
    {

    	HBaseDB db=HBaseDB.getInstance();
    	Table table=db.getTable(ConstantsHBase.TABLE_MOVIE);
    	
    	Scan scan=new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW));
		Filter filter=new PrefixFilter(Bytes.toBytes(movieId+"_"));
		scan.setFilter(filter);
		ResultScanner resultScanner=null;
		try
		{
			resultScanner=table.getScanner(scan);
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList<CrewTO> crewTOs=new ArrayList<CrewTO>();
		CrewTO crewTO=new CrewTO();
		CrewDAO crewDAO=new CrewDAO();
		if(resultScanner!=null)
		{
			Iterator<Result> iter=resultScanner.iterator();
			while(iter.hasNext())
			{
				Result result=iter.next();
				if(result!=null&!result.isEmpty())
				{
					crewTO.setId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW),(Bytes.toBytes( ConstantsHBase.QUALIFIER_MOVIE_CREW_ID)))));
					crewTOs.add(crewDAO.getCrewById(crewTO.getId()));
				}
			}
		}
    	return crewTOs;
    }
    
    public MovieTO getMovieDetailById(int movieId)
    {
    	MovieTO movieTO=getMovieInfoById(movieId);
    	ArrayList<GenreTO> genres=getGenresByMovieId(movieId);
    	//设置genre
    	movieTO.setGenres(genres);
    	CastCrewTO castCrewTO=new CastCrewTO();
    	//设置cast
    	List<CastTO> castList=getCastsByMovieId(movieId);
    	castCrewTO.setCastList(castList);
    	//设置crew
    	List<CrewTO> crewList=getCrewsByMovieId(movieId);
    	castCrewTO.setCrewList(crewList);
    	
    	movieTO.setCastCrewTO(castCrewTO);
		return movieTO;
    	
    }
    
}
