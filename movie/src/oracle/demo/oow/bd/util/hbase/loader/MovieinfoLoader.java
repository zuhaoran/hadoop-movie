package oracle.demo.oow.bd.util.hbase.loader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.hbase.MovieDAO;
import oracle.demo.oow.bd.to.MovieTO;

public class MovieinfoLoader
{
	public static void main(String[] args) {
		MovieinfoLoader loader = new MovieinfoLoader();
		try {
			loader.uploadProfile();
		}
		 catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void uploadProfile() throws Exception {
        FileReader fr = null;
        MovieDAO movieDAO=new MovieDAO();
        try {
            fr = new FileReader(Constant.MOVIE_INFO_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            MovieTO movieTO = null;
            int count = 1;
            while ((jsonTxt = br.readLine()) != null) {
                
                if (jsonTxt.trim().length() == 0)
                    continue;
                
                try {
                    movieTO = new MovieTO(jsonTxt.trim());
                   
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" +
                                       jsonTxt);
                }

                if (movieTO != null) {                 
                	movieDAO.insertMovie(movieTO);
                     System.out.println(count++ + " " + movieTO.getMovieJsonTxt());

                } //EOF if

            } //EOF while
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }
    }
}
