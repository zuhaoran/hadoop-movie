package oracle.demo.oow.bd.dao.hbase;


import java.io.IOException;
import java.lang.reflect.Array;

import java.sql.Connection;
import java.sql.PreparedStatement;

import java.sql.SQLException;

import java.util.ArrayList;

import java.util.Arrays;
import java.util.Iterator;

import java.util.List;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.constant.KeyConstant;
import oracle.demo.oow.bd.dao.MovieDAO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;

import oracle.demo.oow.bd.pojo.SearchCriteria;
import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;
import oracle.kv.KeyValueVersion;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.TableIterator;


import org.apache.avro.Schema;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import org.codehaus.jackson.node.ObjectNode;

public class GenreDAO {

    private static final int TOP = 50;
    private static Table genreTable = null;
    private final static String TABLE_NAME="GENRE";


   
    /**
     * This method insert movie to its different genre group, so that one can
     * query movies by genres. It also assign genres into the genre group so that
     * one can query all the unique genres that exist in the store at a point in
     * time.
     * @param movieTO
     */
    public void insertMovieGenres(MovieTO movieTO) {

        GenreTO genreTO = null;
        int genreId;
        int movieId;
        String genreName = null;        
    	HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_GENRE);
        ArrayList<GenreTO> genreList = null;
        
        if (movieTO != null) {
        	if(table!=null)
            {
        	int index=0;
        	
           
            genreList = movieTO.getGenres();
            Iterator iter = genreList.iterator();
            movieId = movieTO.getId();

            	while (iter.hasNext()) {
            		genreTO = (GenreTO)iter.next();
            		genreId = genreTO.getId();
            		genreTO.setCid(ConstantsHBase.TABLE_GENRE);
            		genreName = genreTO.getName();
                
            		System.out.println("jsn " + genreTO.toJsonString());
            		Put put=new Put(Bytes.toBytes(genreTO.getId()));
            		put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW),Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME),Bytes.toBytes(genreName));
                
            		try
            		{
            			table.put(put);
            		} catch (IOException e)
            		{
            			// TODO Auto-generated catch block
            			e.printStackTrace();
            		}
      
            		
//                /**
//                 * Map movie to its genre using key=/GN_MV/genreId-/movieId
//                 */
//                key = KeyUtil.getGenreMovieKey(genreId, movieId);
//                value = Value.createValue(Constant.EMPTY_PACKET.getBytes());
//                getKVStore().put(key, value);
            	}
            	
            	try
				{
					table.close();
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            } //EOF while
        } //EOF if
    } //insertMovieGenres

    /**
     * This method returns all different Genres available in the store
     * @return List of GenreTO
     */
    public List<GenreTO> getGenres() {

        String genreTOValue = null;        
        GenreTO genreTO = null;
        HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
		List<GenreTO> genreList = new ArrayList<GenreTO>();
		try
		{
			Scan scan = new Scan();
						
			//设置过滤器
			Filter filter = new FamilyFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("info")));
			scan.setFilter(filter);
			ResultScanner resultScanner = table.getScanner(scan);
			  
			for (Result result : resultScanner) {
			}
			
			resultScanner.close();
			table.close();
		} catch (Exception e)
		{
			// TODO: handle exception
		}

      
       
        return genreList;
    }
    public List getGenreById (Integer genreId)
    {
    	List<Cell> cells=null;
    	HBaseDB db=HBaseDB.getInstance();
    	try {
			Table table = db.getTable(ConstantsHBase.TABLE_GENRE);
			Get get = new Get(Bytes.toBytes(genreId));
			//获取版本的数量
			//get.setMaxVersions(1);
			
			//不设置获取整行内容
			//get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"));
			Result result = table.get(get);
			cells = result.getColumnCells(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME));
			/*for (Cell cell : cells) {
				System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
			}*/
			
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return cells;
    }
    public boolean isExist(GenreTO genreTO)	
    {
    	if(getGenreById(genreTO.getId()).size()==0)
    	return false;
    	else
    	return true;
    }
    public void insertGenre(GenreTO genreTO)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	db.put(ConstantsHBase.TABLE_GENRE,genreTO.getId(),ConstantsHBase.FAMILY_GENRE_GENRE,ConstantsHBase.QUALIFIER_GENRE_NAME,genreTO.getName());
    }
    public void insertGenreMovie(MovieTO movieTO,GenreTO genreTO)
    {
    	HBaseDB db=HBaseDB.getInstance();
    	db.put(ConstantsHBase.TABLE_GENRE, genreTO.getId()+"_"+movieTO.getId(),
    			ConstantsHBase.FAMILY_GENRE_MOVIE, ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID, movieTO.getId());
    }
    public List<GenreMovieTO> getGenreMovies() {

        KeyValueVersion keyValue = null;
        
        String movieIdStr = null;
        String genreIdStr = null;
        GenreMovieTO genreMovieTO = null;
        GenreTO genreTO=null;
        MovieTO movieTO=null;
        List<GenreMovieTO> genreMovieList = new ArrayList<GenreMovieTO>();
        HBaseDB db=HBaseDB.getInstance();
        Table table=db.getTable(ConstantsHBase.TABLE_GENRE);
        Scan scan=new Scan();
        scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW));
        try
		{
			ResultScanner resultScanner=table.getScanner(scan);
			if(resultScanner!=null)
			{
				Iterator<Result> iterator=resultScanner.iterator();
				while(iterator.hasNext())
				{
					Result result=iterator.next();
					if(result!=null)
					{
						genreTO.setId(Bytes.toInt(result.getRow()));
						genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE),Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME))));
					}
				}
			}
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return genreMovieList;
    }

}
