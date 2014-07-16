package parsingJson;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

public class DatabaseAccess
{
	private Connection connect = null;
    private Statement statement = null;
    private java.sql.PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;
    private long userId = 0;
    private long geoId = 0;
    
	DatabaseAccess()
	{
		try 
		{
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection("jdbc:mysql://localhost/mturk_survey_statuses?"+
			                        "user=root&password=");
		} 
		catch (ClassNotFoundException e1) 
		{
			System.out.println("Error: class not found");
			return;
		}
		catch (SQLException e)
		{
			System.out.println("Error: SQL Exception "+ e.getMessage());
			return;
		}
	       
	}

	
	public void addTweet(JSONObject o, long user_id, long geo_id)
	{
		try
		{
		preparedStatement = connect
		.prepareStatement("insert into tweets values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		     
			  preparedStatement.setLong(1,(Long) o.get("id"));
		      preparedStatement.setString(2,(String)o.get("id_str"));
		      preparedStatement.setString(3, (String) o.get("text"));
		      preparedStatement.setString(4,(String) o.get("in_reply_to_screen_name"));
		      preparedStatement.setString(5,(String) o.get("in_reply_to_status_id_str"));
		      
		      Long reply_to_status_id = (long) -1;
		      if (o.get("in_reply_to_status_id") != null )
		    	  reply_to_status_id = (Long) o.get("in_reply_to_status_id");
		      
		      preparedStatement.setLong(6, reply_to_status_id);
		      preparedStatement.setString(7,(String) o.get("in_reply_to_user_id_str"));
		      
		      Long in_reply_to_user_id = (long) -1;
		      if (o.get("in_reply_to_user_id") != null )
		    	  reply_to_status_id = (Long) o.get("in_reply_to_user_id");
		      
		      preparedStatement.setLong(8, in_reply_to_user_id);
		      preparedStatement.setString(9, (String) o.get("filter_level"));
		      preparedStatement.setBoolean(10, (Boolean)o.get("retweeted")); 
		      preparedStatement.setLong(11, (Long) o.get("retweet_count"));
		      preparedStatement.setBoolean(12, (Boolean) o.get("favorited"));
		      preparedStatement.setLong(13,(Long) o.get("favorite_count"));
		      preparedStatement.setBoolean(14, (Boolean) o.get("truncated"));
		      preparedStatement.setString(15, (String) o.get("lang"));
		      String date = (String) o.get("created_at");
		      preparedStatement.setString(16,date);
		      //preparedStatement.setString(17, null); // created at formatted
		      preparedStatement.setString(17, (String) o.get("source"));
		      preparedStatement.setString(18, (String) null); // place id
		      preparedStatement.setLong(19, (Long) user_id);
		      preparedStatement.setLong(20, (Long) geo_id);
		      preparedStatement.executeUpdate();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	public void addTweet(ParseLine p, long user_id, long geo_id)
	{
		try
		{
		preparedStatement = connect
		.prepareStatement("insert into tweets values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		     
			  preparedStatement.setLong(1,p.getId());
		      preparedStatement.setString(2,String.valueOf(p.getId()));
		      preparedStatement.setString(3, p.getText());
		      preparedStatement.setString(4,p.getInReplyToScreenName());
		      preparedStatement.setString(5,(String) String.valueOf(p.getInReplyToStatusId()));
		      preparedStatement.setLong(6, p.getInReplyToStatusId());
		      preparedStatement.setString(7,String.valueOf(p.getInReplyToUserId()));
		       preparedStatement.setLong(8, p.getInReplyToUserId());
		      preparedStatement.setString(9, null);
		      preparedStatement.setBoolean(10, p.isRetweetedByMe()); 
		      preparedStatement.setLong(11, p.getRetweetCount());
		      preparedStatement.setBoolean(12,p.isFavorited());
		     preparedStatement.setLong(13,p.getFavoriteCount());
		      preparedStatement.setBoolean(14, p.isTruncated());
		      preparedStatement.setString(15, p.getLang());
		      preparedStatement.setString(16,p.getCreatedAt());
		      preparedStatement.setString(17,p.getSource());
		      preparedStatement.setString(18, (String) null); // place id
		      preparedStatement.setLong(19, (Long) user_id);
		      preparedStatement.setLong(20, (Long) geo_id);
		      preparedStatement.executeUpdate();
		      
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	// geo information
	public long addGeo(JSONObject o)
	{
		JSONObject coordinatesInfo = null;
		JSONArray coordinates = null;
		if(o.get("coordinates") != null){
			coordinatesInfo = (JSONObject) o.get("coordinates");
			coordinates = (JSONArray) coordinatesInfo.get("coordinates");
		}
		
		Double x = null;
		Double y = null;
		if(coordinates != null){
			x = Double.parseDouble(String.valueOf(coordinates.get(1)));
			y = Double.parseDouble(String.valueOf(coordinates.get(0)));
		}
		
		try
		{
		preparedStatement = connect
		.prepareStatement("insert into geo values (?,?,?,?)");
		     
			  preparedStatement.setLong(1,++geoId);
			  if(coordinatesInfo != null)
				  preparedStatement.setString(2, (String) coordinatesInfo.get("type"));
			  else
				  preparedStatement.setString(2, null);
			  preparedStatement.setDouble(3,x);
			  preparedStatement.setDouble(4,y);
			  preparedStatement.executeUpdate();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		
		return geoId;
	}
	
	public long addGeo(ParseLine p)
	{
		
		//JSONObject coordinatesInfo = (JSONObject) o.get("coordinates");
		//JSONArray coordinates = (JSONArray) coordinatesInfo.get("coordinates");
		
		//Double x = Double.parseDouble(String.valueOf(coordinates.get(1)));
		//Double y = Double.parseDouble(String.valueOf(coordinates.get(0)));
		Double x = p.getGeoLocation().getLatitude();
		Double y = p.getGeoLocation().getLongitude();
		try
		{
		preparedStatement = connect
		.prepareStatement("insert into geo values (?,?,?,?)");
		     
			  preparedStatement.setLong(1,++geoId);
			  preparedStatement.setString(2, "point");
			  preparedStatement.setDouble(3,x);
			  preparedStatement.setDouble(4,y);
			  preparedStatement.executeUpdate();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		
		return geoId;
	}
	
	// assuming that the user is from the usa and has geo coordinates
	public long addUser(JSONObject o)
	{
		JSONObject user = (JSONObject) o.get("user");
		try
		{
		preparedStatement = connect.prepareStatement("select userid from users where id_str=\"" + (String)user.get("id_str") + "\"");
		ResultSet rs = preparedStatement.executeQuery();
		if(rs.next()){
			return rs.getLong(1);
				
		}
		
		preparedStatement = connect
		.prepareStatement("insert into users values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		     
			  preparedStatement.setLong(1,++userId);
		      preparedStatement.setLong(2,(Long)user.get("id"));
		      preparedStatement.setString(3, (String) user.get("id_str"));
		      System.out.println(user.get("id_str"));
		      preparedStatement.setLong(4,(Long) user.get("statuses_count"));
		      preparedStatement.setLong(5,(Long) user.get("followers_count"));
		      preparedStatement.setLong(6,(Long) user.get("favourites_count"));
		      preparedStatement.setString(7, (String) user.get("description"));
		      preparedStatement.setString(8, (String) user.get("lang"));
		      preparedStatement.setString(9, null); // profile banner url
		      preparedStatement.setString(10, (String) user.get("location"));
		      preparedStatement.setBoolean(11, (Boolean) user.get("verified"));
		      preparedStatement.setBoolean(12,(Boolean) user.get("contributors_enabled"));
		      preparedStatement.setString(13, (String) user.get("name"));
		      preparedStatement.setString(14, (String) user.get("created_at"));
		      preparedStatement.setBoolean(15,(Boolean) user.get("geo_enabled"));
		      preparedStatement.setString(16, (String) user.get("url"));
		      preparedStatement.setString(17, (String) user.get("time_zone"));
		      preparedStatement.setString(18, (String) user.get("screen_name"));
		      preparedStatement.setLong(19, (Long) user.get("listed_count"));
		      preparedStatement.executeUpdate();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		
		return userId;
	}
	
	public long addUser(ParseLine p)
	{
		//JSONObject user = (JSONObject) o.get("user");
		User_ user = p.getUser();
		try
		{
		preparedStatement = connect
		.prepareStatement("insert into users values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		     
			  preparedStatement.setLong(1,++userId);
		      preparedStatement.setLong(2, user.getUserid());
		      preparedStatement.setString(3, String.valueOf(user.getUserid()));
		      System.out.println(String.valueOf(user.getUserid()));
		      preparedStatement.setLong(4,user.getStatuses_count());
		      preparedStatement.setLong(5,user.getFollowers_count());
		      preparedStatement.setLong(6,user.getFavourites_count());
		      preparedStatement.setString(7, user.getDescription());
		      preparedStatement.setString(8, user.getLang());
		      preparedStatement.setString(9, null); // profile banner url
		      preparedStatement.setString(10, user.getLocation());
		      preparedStatement.setBoolean(11,user.isVerified());
		      preparedStatement.setBoolean(12,user.isContributors_enabled());
		      preparedStatement.setString(13, user.getName());
		      preparedStatement.setString(14, user.getCreated_at());
		      preparedStatement.setBoolean(15,user.isGeo_enabled());
		      preparedStatement.setString(16, user.getUrl());
		      preparedStatement.setString(17, user.getTime_zone());
		      preparedStatement.setString(18, user.getScreen_name());
		      preparedStatement.setLong(19, user.getListed_count());
		      preparedStatement.executeUpdate();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		
		return userId;
	}
	
	public void close() {
	    try {
	      if (resultSet != null) {
	        resultSet.close();
	      }

	      if (statement != null) {
	        statement.close();
	      }

	      if (connect != null) {
	        connect.close();
	      }
	    } catch (Exception e) {

	    }
	  }
	
}
