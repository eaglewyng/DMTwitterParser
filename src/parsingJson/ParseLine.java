package parsingJson;

import twitter4j.*;

public class ParseLine {
	private String createdAt;
	private long id;
	private String text;
	private String source;
	private boolean isTruncated;
	private long inReplyToStatusId;
	private long inReplyToUserId;
	private boolean isFavorited;
	private String inReplyToScreenName;
	private GeoLocation geoLocation = null;
	private long retweetCount;
	private boolean wasRetweetedByMe;
	private User_ user = new User_();
	private long favoriteCount;
	private String lang;
	
	public ParseLine() {

	}

	public void parse(String line) {
		String all[] = line.split(",");
		boolean inuser = false;
		boolean tweetid = false;
		boolean userid = false;
		boolean havetext = false;
		boolean havecreated = false;
		
		
		for (int i = 0; i < all.length; ++i) {
			String[] feature = all[i].split("=");
			//System.out.println(all[i]);
			if (all[i].contains("createdAt=") && !havecreated) {
				createdAt = feature[1];
				havecreated = true;
			}
			if (all[i].contains("id=") && !tweetid) {
				id = Long.parseLong(feature[1]);
				tweetid=true;
			}
			if (all[i].contains("text=") && !havetext) {

				int startindex = line.indexOf("text=");
				int endindex = line.indexOf("source=");
				String textStr = line.substring(startindex,endindex-2);
				
				text = getContent("text=",textStr);
				System.out.println("text: " + text);
				havetext = true;

			}
			if (all[i].contains("source=")) {
				source = getContent("source=", all[i]);
			}
			if (all[i].contains("isTruncated=")) {
				isTruncated = Boolean.getBoolean(feature[1]);
			}
			if (all[i].contains("inReplyToStatusId=")) {
				inReplyToStatusId = Long.parseLong(feature[1]);
			}
			if (all[i].contains("inReplyToUserId=")) {
				inReplyToUserId = Long.parseLong(feature[1]);
			}
			if (all[i].contains("isFavorited=")) {
				isFavorited = Boolean.getBoolean(feature[1]);
			}
			if (all[i].contains("inReplyToScreenName=")) {
				inReplyToScreenName = feature[1].substring(1,
						feature[1].length() - 1);
			}
			if (all[i].contains("geoLocation=")) {
				geoLocation = null;
				if (!feature[1].equals("null")) {
					
					Double lon = Double.parseDouble(feature[2]);
					++i;
					feature = all[i].split("=");
					Double lat = Double.parseDouble(feature[1].substring(0,
							feature[1].length() - 1));
					
					geoLocation = new GeoLocation(lon,lat);
				}
			}
			if (all[i].contains("retweetCount=")) {
				retweetCount = Long.parseLong(feature[1]);
			}
			if (all[i].contains("wasRetweetedByMe=")) {
				wasRetweetedByMe = Boolean.getBoolean(feature[1]);
			}
			
			if (all[i].contains("favoriteCount=")) {
				favoriteCount = Long.parseLong(feature[1]);
			}

			if (all[i].contains("lang=")) {
				//System.out.println("feature: "+all[i]);
				lang = feature[1].substring(1,feature[1].length() - 1);
			}

			if (all[i].contains("user=UserJSONImpl")) {
				inuser = true;
			}
			if (inuser) {
				
				if (all[i].contains("id=") && !userid) {
					user.setUserid(Long.parseLong(feature[2]));
					userid = true;
				}
				
				if (all[i].contains("statusesCount=")) {
					int u = 0;
					user.setStatuses_count(Long.parseLong(feature[1]));
				}
				
				if (all[i].contains("FollowersCount=")) {
					user.setFollowers_count(Long.parseLong(feature[1]));
				}
				
				if (all[i].contains("favouritesCount=")) {
					user.setFavourites_count(Long.parseLong(feature[1]));
				}
				
				if (all[i].contains("description=")) {
					
					int startindex = line.indexOf("description=");
					int endindex = line.indexOf("isContributorsEnabled");
					String descStr = line.substring(startindex,endindex-2);
					//System.out.println(descStr);
					user.setDescription(getContent("description=",descStr));
				}
				
				if (all[i].contains("lang=")) {
					user.setLang(feature[1].substring(1,feature[1].length() - 1));
				}
				
				if (all[i].contains("location=")) {
					
					user.setLocation(getContent("location=",all[i]));
				}
				
				if (all[i].contains("isVerified=")) {
					user.setVerified(Boolean.getBoolean(feature[1]));
				}
			
				if (all[i].contains("isContributorsEnabled=")) {
					user.setContributors_enabled(Boolean.getBoolean(feature[1]));
				}
				
				if (all[i].contains("name=") && !all[i].contains("UserMentionEntityJSONImpl")) {
				
					String name = getContent("name=", all[i]);
					
					user.setName(name);
				}
				
				if (all[i].contains("createdAt=")) {
					user.setCreated_at(feature[1]);
				}
				
				if (all[i].contains("isGeoEnabled=")) {
					user.setGeo_enabled(Boolean.getBoolean(feature[1]));
				}
				
				if (all[i].contains("url=")) {
					String u = getContent("url=", all[i]);
					user.setUrl(u);
				}
				
				if (all[i].contains("timeZone=")) {
					user.setTime_zone(feature[1].substring(1,feature[1].length() - 1));
				}
				
				if (all[i].contains("screenName=")) {
					String sname = getContent("screenName=", all[i]);
					user.setScreen_name(sname);
				}
				
				if (all[i].contains("listedCount=")) {
					user.setListed_count(Long.parseLong(feature[1]));
				}
			}
		}
	}
	
	private String getContent(String title, String phrase)
	{
		System.out.println("before: " + phrase);
		int index = phrase.indexOf(title);
		StringBuilder sb = new StringBuilder();
		for ( int j = index+title.length()+1; j< phrase.length(); ++j)
		{
			if (j == phrase.length() - 1 && phrase.charAt(j) != '\'')
				sb.append(phrase.charAt(j));
			else if ( j < phrase.length()-1)
				sb.append(phrase.charAt(j));
		}
		System.out.println("after: " + sb.toString());
		return sb.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	public String getCreatedAt() {
		return this.createdAt;
	}

	/**
	 * {@inheritDoc}
	 */
	public long getId() {
		return this.id;
	}

	/**
	 * {@inheritDoc}
	 */
	public String getText() {
		return this.text;
	}

	/**
	 * {@inheritDoc}
	 */
	public String getSource() {
		return this.source;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isTruncated() {
		return isTruncated;
	}

	/**
	 * {@inheritDoc}
	 */
	public long getInReplyToStatusId() {
		return inReplyToStatusId;
	}

	/**
	 * {@inheritDoc}
	 */
	public long getInReplyToUserId() {
		return inReplyToUserId;
	}

	/**
	 * {@inheritDoc}
	 */
	public String getInReplyToScreenName() {
		return inReplyToScreenName;
	}

	/**
	 * {@inheritDoc}
	 */
	public GeoLocation getGeoLocation() {
		return geoLocation;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isFavorited() {
		return isFavorited;
	}

	/**
	 * {@inheritDoc}
	 */
	public User_ getUser() {
		return user;
	}



	/**
	 * {@inheritDoc}
	 */
	public long getRetweetCount() {
		return retweetCount;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isRetweetedByMe() {
		return wasRetweetedByMe;
	}

	public long getFavoriteCount() {
		return favoriteCount;
	}

	public void setFavoriteCount(long favoriteCount) {
		this.favoriteCount = favoriteCount;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

}
