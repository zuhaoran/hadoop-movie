package oracle.demo.oow.bd.dao.hbase;


import java.io.IOException;
import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import java.util.List;

import oracle.demo.oow.bd.dao.hbase.MovieDAO;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;

import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.KeyUtil;

import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import org.codehaus.jackson.node.ObjectNode;

public class CastDAO {

    private final static String TABLE_NAME="CAST";
    public void insertCastInfo(CastTO castTO)
    {
    	HBaseDB db =HBaseDB.getInstance();
    	db.put(ConstantsHBase.TABLE_CAST, castTO.getId(), ConstantsHBase.FAMILY_CAST_CAST, 
    			ConstantsHBase.QUALIFIER_CAST_NAME, castTO.getName());
    	insertCastToMovie(castTO);
    }
    private void insertCastToMovie(CastTO castTO)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	List<CastMovieTO> movieTOs=castTO.getCastMovieList();
    	MovieDAO movieDAO=new MovieDAO();
    	for (CastMovieTO castMovieTO : movieTOs)
    	{
    		db.put(ConstantsHBase.TABLE_CAST, castTO.getId()+"_"+castMovieTO.getId(), ConstantsHBase.FAMILY_CAST_MOVIE,ConstantsHBase.QUALIFIER_CAST_MOVIE_ID , castMovieTO.getId());
    		db.put(ConstantsHBase.TABLE_CAST, castTO.getId()+"_"+castMovieTO.getId(), ConstantsHBase.FAMILY_CAST_MOVIE,ConstantsHBase.QUALIFIER_CAST_CHARACTER , castMovieTO.getCharacter());
    		db.put(ConstantsHBase.TABLE_CAST, castTO.getId()+"_"+castMovieTO.getId(), ConstantsHBase.FAMILY_CAST_MOVIE,ConstantsHBase.QUALIFIER_CAST_ORDER , castMovieTO.getOrder());
    		movieDAO.insertMovieCast(castTO,castMovieTO.getId());
    	}
    }
    public CastTO getCastById(int castId)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	CastTO castTO=new CastTO();
    	castTO.setId(castId);
    	try
		{
			Result result= db.get(ConstantsHBase.TABLE_CAST, castId, ConstantsHBase.FAMILY_CAST_CAST, null);
			if(result!=null)
			{
				castTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME))));
			}
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	ArrayList<CastMovieTO> castMovieTOs=new ArrayList<CastMovieTO>();
    	CastMovieTO castMovieTO=new CastMovieTO();
    	Table table=db.getTable(ConstantsHBase.TABLE_CAST);
    	Scan scan=new Scan();
    	scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE));
    	Filter filter=new PrefixFilter(Bytes.toBytes(castId+"_"));
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
						castMovieTO.setId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID))));
						castMovieTO.setCharacter(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_CHARACTER))));
						castMovieTO.setOrder(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_ORDER))));
						castMovieTOs.add(castMovieTO);
					}
				}
			}
		table.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	castTO.setCastMovieList(castMovieTOs);
    	return castTO;
    }
    public List<MovieTO> getMoviesByCast(int castId) {
        List<CastMovieTO> castMovieList = null;
        List<MovieTO> movieList = new ArrayList<MovieTO>();
        CastMovieTO castMovieTO=new CastMovieTO();
        CastTO castTO = null;
        MovieDAO movieDAO = new MovieDAO();
        castTO=getCastById(castId);
        castMovieList=castTO.getCastMovieList();
        Iterator<CastMovieTO> iterator=castMovieList.iterator();
        while(iterator.hasNext())
        {
        	castMovieTO=iterator.next();
        	movieList.add(movieDAO.getMovieById(castMovieTO.getId()));
        }
        /*for (MovieTO movieTO2 : movieList)
		{
        	movieList.add(movieTO2);
		}*/
        
        return movieList;

    }
 
    public List<CastTO> getMovieCasts(int movieId) {

        List<CastTO> castList = null;

        CastCrewTO castCrewTO = null;
        MovieTO movieTO=null;
        MovieDAO movieDAO=new MovieDAO();
        if (movieId > -1) {
        	movieTO=  movieDAO.getMovieById(movieId);
        	castCrewTO=movieTO.getCastCrewTO();
        	castList=castCrewTO.getCastList();
        } //if (movieId > -1)


        return castList;
    } //getMovieCasts


}
