package parsingJson;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


import java.io.PrintWriter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class Parse
{
	
	private static DatabaseAccess db;

	public static void main(String[] args)
	{
		
		if(args.length > 0){
			if(args[0].equals("-w")){
				
			}
			else if (args[0].equals("-j")){
				
			}
			
		}
		db = new DatabaseAccess();
		long  tweets = 0;
		int good = 0;
		int bad = 0;
		
		PrintWriter errorWriter = null;
		try{
			errorWriter = new PrintWriter("ParseLogs/bad.txt");
		}
		catch(IOException e){
			e.printStackTrace();
			System.out.println("The logger could not be initiated!");
		}
		
		
		try 
		{
				File dir = new File("/home/pbridd/dml/DMTwitterParser/data");
				File[] data = dir.listFiles();
				for ( int i = 0; i < data.length; i++)
				{
					
					File file = new File(data[i].getAbsolutePath());
					
					BufferedReader reader  = new BufferedReader(new FileReader(file));
					// jason parser declaration
					JSONParser parser = new JSONParser();
					
					boolean isInFile = true;
					while(isInFile)
					{
						
						String line;
						
						// jason parser read line
						 line = reader.readLine();
						
						
						
						if ( line != null)
						{
							
							//System.out.println(line);
							try{
								++tweets;
								// jason parser code with different if statement
								JSONObject o = (JSONObject) parser.parse(line);
								
								//json good stuff
								
								if(o.get("coordinates") != null){
									JSONObject coordinatesInfo = (JSONObject) o.get("coordinates");
									JSONArray coordinates = (JSONArray) coordinatesInfo.get("coordinates");
									Double x = Double.parseDouble(String.valueOf(coordinates.get(1)));
									Double y = Double.parseDouble(String.valueOf(coordinates.get(0)));
								}
								
								
								
								// end of json good stuff
	
								
								//System.out.println(line);
								// json parser methods
								
								Long userId = db.addUser(o);
								Long geoId = db.addGeo(o);
								db.addTweet(o, userId, geoId);
								++good;
							}
							//what to do if a bad tweet is found
							catch (ParseException e)
							{
								bad++;
								//print log to file if the file is open to the printwriter
								if(errorWriter != null){
									errorWriter.print("Couldn't parse tweet "+ tweets + ":\n");
									errorWriter.println(line);
								}
								else{
									e.printStackTrace();
									System.out.println("Error: could not parse");
								}
							}
							
						
						  
						}
						else
							isInFile = false;
					}
					
				}
			
				System.out.println("There were " + tweets + " tweets found in total.");
				System.out.println("There were " + good + " good tweets found.");
				System.out.println("There were " + bad + " bad tweets found.");
				System.out.println("Successfully parsed " + ((double)good/(double)tweets)*100 + "% of the tweets.");
				db.close();
			
		} 
		catch (FileNotFoundException e) 
		{
			System.out.println("Error: Could not open file");
		}
		catch (IOException e)
		{
			System.out.println("Error: could not readLine()");
		}// json catch


	}
	
	


}
