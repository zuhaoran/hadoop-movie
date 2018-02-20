package oracle.demo.oow.bd.util.hbase.loader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.hbase.CrewDAO;
import oracle.demo.oow.bd.to.CrewTO;

public class CrewLoader
{
	public static void main(String[] args) {
		CrewLoader loader = new CrewLoader();
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
        CrewDAO crewDAO=new CrewDAO();
        try {
            fr = new FileReader(Constant.WIKI_MOVIE_CREW_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            CrewTO crewTO = null;
            int count = 1;
            while ((jsonTxt = br.readLine()) != null) {
                
                if (jsonTxt.trim().length() == 0)
                    continue;
                
                try {
                	crewTO = new CrewTO(jsonTxt.trim());
                   
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" +
                                       jsonTxt);
                }

                if (crewTO != null) {                 
                	crewDAO.insertCrewInfo(crewTO);
                     System.out.println(count++ + " " + crewTO.getJsonTxt());

                } //EOF if

            } //EOF while
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }
    }
}
