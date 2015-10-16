package Saddahaq;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.lucene.ValueContext;

public class User_node {// implements User_nodeService.Iface{
	
	private static GraphDatabaseService graphDb = null;
	private static HashMap<String,String> indexFullName = new HashMap<String,String>();
	
	public static Comparator<Node> TimeCreatedComparatorForNodes = new Comparator<Node>() {

		public int compare(Node n1, Node n2) {
		   int v1 = 0;
		   int v2 = 0;
		   if(n1.hasProperty("time_created"))
			   v1 = Integer.parseInt(n1.getProperty("time_created").toString());
		   if(n2.hasProperty("time_created"))
			   v2 = Integer.parseInt(n2.getProperty("time_created").toString());
		   //ascending order
		   //return v1-v2;
		   //descending order
		   return v2-v1;
	    }};
		
	public static Comparator<Relationship> TimeCreatedComparatorForRelationships = new Comparator<Relationship>() {

		public int compare(Relationship n1, Relationship n2) {
		   int v1 = 0;
		   int v2 = 0;
		   if(n1.hasProperty("time"))
			   v1 = Integer.parseInt(n1.getProperty("time").toString());
		   if(n2.hasProperty("time"))
			   v2 = Integer.parseInt(n2.getProperty("time").toString());
		   //ascending order
		   //return v1-v2;
		   //descending order
		   return v2-v1;
	    }}; 
	    

	public static Comparator<Node> WeightComparatorForSpaces = new Comparator<Node>() {

		public int compare(Node n1, Node n2) {
		   int v1 = n1.getDegree();
		   int v2 = n2.getDegree();
		   
		   //ascending order
		   //return v1-v2;
		   //descending order
		   return v2-v1;
	    }};
	    

	public class ItemIdWeight implements Comparable<ItemIdWeight> {

		private String id;
		private int weight;
		public ItemIdWeight(String id, int weight)
		{
			this.id = id;
			this.weight = weight;
		}

		@Override
		public int compareTo(ItemIdWeight other) {
			// TODO Auto-generated method stub
			return other.weight - this.weight;
		}};

	private Node tweet(String id, int pos, int neg, int neu, String tweet_text, String time_analysis){
		Node node = graphDb.createNode();
		node.setProperty("id",id);
		node.setProperty("pos",pos);
	    node.setProperty("neg",neg);
	    node.setProperty("neu",neu);
	    node.setProperty("tweet_text",tweet_text);
	    node.setProperty("time_analysis",time_analysis);
	    node.setProperty("__CLASS__","Saddahaq.tweet");
	    return node;
	}
	
	private Node pos(String pos_id, int last_update){
		Node node = graphDb.createNode();
		node.setProperty("pos_id",pos_id);
		node.setProperty("last_update",last_update);
	    node.setProperty("__CLASS__","Saddahaq.pos");
	    return node;
	}
	
	private Node user(String first_name, String last_name, String user_name, String email, String location, int previlege,
				int time_created, int weight, int spam_weight, String nouns, String feed_subscription, int notify_time,
				int last_seen, String fav_hash, String acc_type, String pins){
		Node node = graphDb.createNode();
		node.setProperty("first_name",first_name);
		node.setProperty("last_name",last_name);
	    node.setProperty("user_name",user_name);
	    node.setProperty("email",email);
	    node.setProperty("location",location);
	    node.setProperty("previlege",previlege);
	    node.setProperty("time_created",time_created);
	    node.setProperty("weight",weight);
	    node.setProperty("spam_weight",spam_weight);
	    node.setProperty("nouns",nouns);
	    node.setProperty("feed_subscription",feed_subscription);
	    node.setProperty("notify_time",notify_time);
	    node.setProperty("last_seen",last_seen);
	    node.setProperty("fav_hash",fav_hash);
	    node.setProperty("acc_type",acc_type);
	    node.setProperty("pins",pins);
	    node.setProperty("__CLASS__","Saddahaq.user");
	    return node;
	}
	
	private Node user_tiles(String id, String news, String news_Personalized){
		Node node = graphDb.createNode();
		node.setProperty("id",id);
		node.setProperty("news",news);
	    node.setProperty("news_Personalized",news_Personalized);
	    node.setProperty("__CLASS__","Saddahaq.user_tiles");
	    return node;
	}
	
	private Node avg_wt(String id, int average, int total, int top, int top_10, int top_20, int top_30, String top_30_users, int threshold_weight){
		Node node = graphDb.createNode();
		node.setProperty("id",id);
		node.setProperty("average",average);
	    node.setProperty("total",total);
	    node.setProperty("top",top);
	    node.setProperty("top_10",top_10);
	    node.setProperty("top_20",top_20);
	    node.setProperty("top_30",top_30);
	    node.setProperty("top_30_users",top_30_users);
	    node.setProperty("threshold_weight",threshold_weight);
	    node.setProperty("__CLASS__","Saddahaq.avg_wt");
	    return node;
	}
	
	private Node item_trending_weights(String id, String weights, float zscore, float stddev, float stddev_change){
		Node node = graphDb.createNode();
		node.setProperty("id",id);
		node.setProperty("weights",weights);
	    node.setProperty("zscore",zscore);
	    node.setProperty("stddev",stddev);
	    node.setProperty("stddev_change",stddev_change);
	    node.setProperty("__CLASS__","Saddahaq.item_trending_weights");
	    return node;
		}

	private Node trending_calc_timings(String id, String timing_list){
		Node node = graphDb.createNode();
		node.setProperty("id",id);
		node.setProperty("timing_list",timing_list);
	    node.setProperty("__CLASS__","Saddahaq.trending_calc_timings");
	    return node;
	}

	private Node quickpost(String qp_id, String qp_content, int time_created, int weight, int spam_weight){
		Node node = graphDb.createNode();
		node.setProperty("qp_id",qp_id);
		node.setProperty("qp_content",qp_content);
		node.setProperty("time_created",time_created);
		node.setProperty("weight",weight);
		node.setProperty("spam_weight",spam_weight);
	    node.setProperty("__CLASS__","Saddahaq.quickpost");
	    return node;
	}
	
	private Node debate_suggestion(String d_id, String d_topic, int time_created, int weight){
		Node node = graphDb.createNode();
		node.setProperty("d_id",d_id);
		node.setProperty("d_topic",d_topic);
	    node.setProperty("time_created",time_created);
	    node.setProperty("weight",weight);
	    node.setProperty("__CLASS__","Saddahaq.debate_suggestion");
	    return node;
	}
	
	private Node townhall_suggestion(String t_id, String t_topic, String t_celebrity, int time_created, int weight){
		Node node = graphDb.createNode();
		node.setProperty("t_id",t_id);
		node.setProperty("t_topic",t_topic);
	    node.setProperty("t_celebrity",t_celebrity);
	    node.setProperty("time_created",time_created);
	    node.setProperty("weight",weight);
	    node.setProperty("__CLASS__","Saddahaq.townhall_suggestion");
	    return node;
	}

	private Node townhall(String t_id, String t_title, String t_title_id, String t_content, String t_img_url, int t_date, 
					int t_duration, int weight, int time_created, int approved, int head, int space, int skip_from_special_tiles,
					String lang, String guest_user_names, String guest_full_names){
		Node node = graphDb.createNode();
		node.setProperty("t_id",t_id);
		node.setProperty("t_title",t_title);
	    node.setProperty("t_title_id",t_title_id);
	    node.setProperty("t_content",t_content);
	    node.setProperty("t_img_url",t_img_url);
	    node.setProperty("t_date",t_date);
	    node.setProperty("t_duration",t_duration);
	    node.setProperty("weight",weight);
	    node.setProperty("time_created",time_created);
	    node.setProperty("approved",approved);
	    node.setProperty("head",head);
	    node.setProperty("space",space);
	    node.setProperty("skip_from_special_tiles",skip_from_special_tiles);
	    node.setProperty("lang",lang);
	    node.setProperty("guest_user_names",guest_user_names);
	    node.setProperty("guest_full_names",guest_full_names);
	    node.setProperty("__CLASS__","Saddahaq.townhall");
	    return node;
		
	}

	private Node petition(String p_type, String p_id, String p_title, String p_title_id, String p_content, String p_img_url,
					String p_to, int p_target, int p_count, int time_created, int end_date, int weight, int spam_weight,
					int views, String latest_views, int approved, int head, int space, int skip_from_special_tiles, String lang){
		Node node = graphDb.createNode();
		node.setProperty("p_type",p_type);
		node.setProperty("p_id",p_id);
	    node.setProperty("p_title",p_title);
	    node.setProperty("p_title_id",p_title_id);
	    node.setProperty("p_content",p_content);
	    node.setProperty("p_img_url",p_img_url);
	    node.setProperty("p_to",p_to);
	    node.setProperty("p_target",p_target);
	    node.setProperty("p_count",p_count);
	    node.setProperty("time_created",time_created);
	    node.setProperty("end_date",end_date);
	    node.setProperty("weight",weight);
	    node.setProperty("spam_weight",spam_weight);
	    node.setProperty("views",views);
	    node.setProperty("latest_views",latest_views);
	    node.setProperty("approved",approved);
	    node.setProperty("head",head);
	    node.setProperty("space",space);
	    node.setProperty("skip_from_special_tiles",skip_from_special_tiles);//Personalized tiles, trending tiles, stream, context, hashtag, and search result
	    node.setProperty("lang",lang);
	    node.setProperty("__CLASS__","Saddahaq.petition");
	    return node;
	}

	private Node debate(String d_id, String d_title, String d_title_id, String d_img_url, String d_content, String d_criteria,
				  int d_duration, int d_date, int time_created, int weight, int spam_weight, int views, int approved,
				  int head, int space, int skip_from_special_tiles, String lang){

		Node node = graphDb.createNode();
		node.setProperty("d_id",d_id);
		node.setProperty("d_title",d_title);
	    node.setProperty("d_title_id",d_title_id);
	    node.setProperty("d_img_url",d_img_url);
	    node.setProperty("d_content",d_content);
	    node.setProperty("d_criteria",d_criteria);
	    node.setProperty("d_duration",d_duration);
	    node.setProperty("d_date",d_date);
	    node.setProperty("time_created",time_created);
	    node.setProperty("weight",weight);
	    node.setProperty("spam_weight",spam_weight);
	    node.setProperty("views",views);
	    node.setProperty("approved",approved);
	    node.setProperty("head",head);
	    node.setProperty("space",space);
	    node.setProperty("skip_from_special_tiles",skip_from_special_tiles); 
	    node.setProperty("__CLASS__","Saddahaq.debate");
	    return node;

	}
	
	private Node event(String event_id, String event_title_id, String event_title, String event_content, String event_summary, String event_featured_img,
				int event_date_time, int event_date_time_closing, int event_limit, String event_location, int time_created, int weight, int spam_weight,
				int views, String latest_views, int approved, int head, int space, int skip_from_special_tiles, String lang){
		Node node = graphDb.createNode();
		node.setProperty("event_id",event_id);
		node.setProperty("event_title_id",event_title_id);
		node.setProperty("event_title",event_title);
		node.setProperty("event_content", event_content);
		node.setProperty("event_summary",event_summary);
		node.setProperty("event_featured_img",event_featured_img);
		node.setProperty("event_date_time",event_date_time);
		node.setProperty("event_date_time_closing",event_date_time_closing);
		node.setProperty("event_limit",event_limit);
		node.setProperty("event_location",event_location);
		node.setProperty("time_created",time_created);
		node.setProperty("weight",weight);
		node.setProperty("spam_weight",spam_weight);
		node.setProperty("views",views);
		node.setProperty("latest_views",latest_views);
		node.setProperty("approved",approved);
		node.setProperty("head",head);
		node.setProperty("space",space);
		node.setProperty("skip_from_special_tiles",skip_from_special_tiles);//Personalized tiles, trending tiles, stream, context, hashtag, and search result
		node.setProperty("lang",lang);
		node.setProperty("__CLASS__","Saddahaq.event");
		return node;
	}

/*	private Node category(String name, String pins, String exclusive, String other_pins){
		Node node = graphDb.createNode();
		node.setProperty("name",name);
		node.setProperty("pins",pins);
		node.setProperty("exclusive",exclusive);
		node.setProperty("other_pins",other_pins);
	    node.setProperty("__CLASS__","Saddahaq.category");
	    return node;
	} */

	private Node feed(String id, String article, String event, String petition, String townhall, String debate, int time){
		Node node = graphDb.createNode();
		node.setProperty("id",id);
		node.setProperty("article",article);
		node.setProperty("event",event);
		node.setProperty("petition",petition);
		node.setProperty("townhall",townhall);
		node.setProperty("debate",debate);
		node.setProperty("time",time);
	    node.setProperty("__CLASS__","Saddahaq.feed");
	    return node;
	}

	private Node sub_category(String name, int time_created, int weight){
		Node node = graphDb.createNode();
		node.setProperty("name",name);
		node.setProperty("time_created",time_created);
		node.setProperty("weight",weight);
	    node.setProperty("__CLASS__","Saddahaq.sun_category");
	    return node;
	}
	
/*	private Node topic(String name, String sub_topics){
		Node node = graphDb.createNode();
		node.setProperty("name",name);
		node.setProperty("sub_topics",sub_topics);
	    node.setProperty("__CLASS__","Saddahaq.topic");
	    return node;
	}*/

	private Node poll(String id, String poll_question, int poll_status, int time_created){
		Node node = graphDb.createNode();
		node.setProperty("id",id);
		node.setProperty("poll_question",poll_question);
		node.setProperty("poll_status",poll_status);
		node.setProperty("time_created",time_created);
	    node.setProperty("__CLASS__","Saddahaq.poll");
	    return node;
	}
	
	private Node trending( String trend_id){
		Node node = graphDb.createNode();
		node.setProperty("trend_id",trend_id);
	    node.setProperty("__CLASS__","Saddahaq.trending");
	    return node;
	}
	
	private Node article(String article_id, String article_title_id, String article_title, String article_content, String article_summary,
			   String article_featured_img, int time_created, int weight, int spam_weight, int views, int weight_review,
			   String latest_views, int stars, int approved, int head, int space, int skip_from_special_tiles, String lang){
		Node node = graphDb.createNode();
		node.setProperty("article_id",article_id);
		node.setProperty("article_title_id",article_title_id);
		node.setProperty("article_title",article_title);
		node.setProperty("article_content",article_content);
		node.setProperty("article_summary",article_summary);
		node.setProperty("article_featured_img",article_featured_img);
		node.setProperty("time_created",time_created);
		node.setProperty("weight",weight);
		node.setProperty("spam_weight",spam_weight);
		node.setProperty("views",views);
		node.setProperty("weight_review",weight_review);
		node.setProperty("latest_views",latest_views);
		node.setProperty("stars",stars);
		node.setProperty("approved",approved);
		node.setProperty("head",head);
		node.setProperty("space",space);
		node.setProperty("skip_from_special_tiles",skip_from_special_tiles);
		node.setProperty("lang",lang);
	    node.setProperty("__CLASS__","Saddahaq.article");
	    return node;
	}
	
	private Node space(String space_id, String space_title_id, String space_title, String space_tagline,
			 String space_content, String space_featured_img, int time_created, int pins, int closed){
		Node node = graphDb.createNode();
		node.setProperty("space_id",space_id);
		node.setProperty("space_title_id",space_title_id);
		node.setProperty("space_title",space_title);
		node.setProperty("space_tagline",space_tagline);
		node.setProperty("space_content",space_content);
		node.setProperty("space_featured_img",space_featured_img);
		node.setProperty("time_created",time_created);
		node.setProperty("pins",pins);
		node.setProperty("closed",closed);
	    node.setProperty("__CLASS__","Saddahaq.space");
	    return node;
	}
	
	private Node cfpost(String user, String cf_id, String cf_title, String cf_content, String cf_featured_img, String cf_url,
			  String cf_tags, int time_created, int end_date, int amt_target, int amt_raised, int ppl_count){
		Node node = graphDb.createNode();
		node.setProperty("user",user);
		node.setProperty("cf_id",cf_id);
		node.setProperty("cf_title",cf_title);
		node.setProperty("cf_content",cf_content);
		node.setProperty("cf_featured_img",cf_featured_img);
		node.setProperty("cf_url",cf_url);
		node.setProperty("cf_tags",cf_tags);
		node.setProperty("time_created",time_created);
		node.setProperty("end_date",end_date);
		node.setProperty("amt_target",amt_target);
		node.setProperty("amt_raised",amt_raised);
		node.setProperty("ppl_count",ppl_count);
		node.setProperty("__CLASS__","Saddahaq.cf_tags");
		return node;
	}
	
	private Node search_word(String name){
		Node node = graphDb.createNode();
		node.setProperty("name",name);
		node.setProperty("__CLASS__","Saddahaq.search_word");
		return node;
	}
	
	private Node tiles(String id, String value){
		Node node = graphDb.createNode();
		node.setProperty("id",id);
		node.setProperty("value",value);
		node.setProperty("__CLASS__","Saddahaq.tiles");
		return node;
	}
	
	private Node featured_tiles(String id, String value){
		Node node = graphDb.createNode();
		node.setProperty("id",id);
		node.setProperty("value",value);
		node.setProperty("__CLASS__","Saddahaq.featured_tiles");
		return node;
	}
	
	private Node headlines(String id, String value){
		Node node = graphDb.createNode();
		node.setProperty("id",id);
		node.setProperty("value",value);
		node.setProperty("__CLASS__","Saddahaq.headlines");
		return node;
	}
	
	private Node comment(String comment_id, String comment_content, int time_created, int weight, int spam_weight){
		Node node = graphDb.createNode();
		node.setProperty("comment_id",comment_id);
		node.setProperty("comment_content",comment_content);
		node.setProperty("time_created",time_created);
		node.setProperty("weight",weight);
		node.setProperty("spam_weight",spam_weight);
		node.setProperty("__CLASS__","Saddahaq.comment");
		return node;
	} 
	
	private Node lock(String name){
		Node node = graphDb.createNode();
		node.setProperty("name", name);
		node.setProperty("__CLASS__", "Saddahaq.lock");
		return node;
	}
	
	private Node location(String location_id, String cities, String tiles){
		 Node node = graphDb.createNode();
		 node.setProperty("location_id", location_id);
		 node.setProperty("cities", cities);
		 node.setProperty("tiles", tiles);
		 node.setProperty("__CLASS__", "Saddahaq.location");
		 return node;
	}
	
	public User_node()
	{
		if(graphDb == null)
			initGraphDb();
		indexFullName.put("A", "article");
		indexFullName.put("a", "article");
		indexFullName.put("ar", "article");
		indexFullName.put("AR", "article");
		indexFullName.put("E", "event");
		indexFullName.put("e", "event");
		indexFullName.put("ev", "event");
		indexFullName.put("EV", "event");
		indexFullName.put("Q", "quickpost");
		indexFullName.put("q", "quickpost");
		indexFullName.put("C", "comment");
		indexFullName.put("c", "comment");
		indexFullName.put("U", "user");
		indexFullName.put("u", "user");
		indexFullName.put("H", "sub_category");
		indexFullName.put("h", "sub_category");
		indexFullName.put("P", "petition");
		indexFullName.put("p", "petition");
		indexFullName.put("pe", "petition");
		indexFullName.put("PE", "petition");
		indexFullName.put("t", "townhall");
		indexFullName.put("T", "townhall");
		indexFullName.put("to", "townhall");
		indexFullName.put("TO", "townhall");
		indexFullName.put("d", "debate");
		indexFullName.put("D", "debate");
		indexFullName.put("de", "debate");
		indexFullName.put("DE", "debate");
		indexFullName.put("s", "space");
		indexFullName.put("S", "space");
	}
	
	private static void initGraphDb()
	{
		//db path
		String storeDir = "/var/n4j/data/graph.db";
		
		//starting graph database with caonfiguration
		graphDb = new GraphDatabaseFactory()
	    .newEmbeddedDatabaseBuilder( storeDir )
	    .setConfig(GraphDatabaseSettings.allow_store_upgrade, "true")
	    .setConfig(GraphDatabaseSettings.keep_logical_logs, "2 days")
	    .newGraphDatabase();
		
		//register safe shutdown while exiting
		registerShutdownHook( graphDb );
		
		//create indexes if any (node index and relation index)
		String[] nodeIndexNames = {
									"user",
									"user_weight",
									"article",
									"user_tiles",
									"featured_tiles",
									"decommissioned_tiles",
									"headlines",
									"article_content",
									"space_content",
									"article_title",
									"article_weight",
									"event",
									"event_content",
									"event_weight",
									"event_title",
									"poll",
									"space",
									"cfpost",
									"feed",
									"quickpost",
									"quickpost_content",
									"petition",
									"petition_content",
									"debate",
									"debate_suggestion",
									"townhall_suggestion",
									"debate_content",
									"townhall",
									"townhall_content",
									"quickpost_weight",
									"petition_weight",
									"debate_weight",
									"trending",
//									"comment",
									"comment_weight",
									"category",
									"sub_category", //actual hashtag
									"topic",
									"hash_weight",
									"search_word",
									"avg_weights",
									"location",
									"pos",
									"tweet",
									"tiles",
									"lock", //used for synchronization: for more details visit --> http://neo4j.com/docs/stable/tutorials-java-embedded-unique-nodes.html#tutorials-java-embedded-unique-pessimistic
									"trending_calc_times"
									};
		
		String[] relationIndexNames = {
										"comment_vote",
										"comment_voteup",
										"comment_votedown",
										"quickpost_voteup",
										"article_markfav",
										"article_readlater",
										"event_readlater",
										"petition_readlater",
										"debate_readlater",
										"townhall_readlater",
										"event_response",
										"article_voteup",
										};
		
		//customConfiguration for indexes
		Map<String, String> customConfig = new HashMap<String,String>();
		customConfig.put("provider", "lucene");
		customConfig.put("type", "fulltext");
		
		
		try (Transaction tx = graphDb.beginTx())
		{
			//create indexes for nodes
			for(String nodeIndexName: nodeIndexNames)
				graphDb.index().forNodes(nodeIndexName, customConfig);
				
			//create indexes for relations
			for(String relationIndexName: relationIndexNames)
				graphDb.index().forRelationships(relationIndexName, customConfig);
			
			tx.success();
		}
		catch(Exception e){System.out.println("Failed to create indexes");}
		finally{}
	}
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    } );
	}

	public boolean create_user(String first_name, String last_name,
			String user_name, String email, String location, int previlege,
			int time_created, int weight, String acc_type) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx, "user");

			Index<Node> userId_index = graphDb.index().forNodes("user");
			Index<Node> first_name_index = graphDb.index().forNodes("firstname");
			Index<Node> email_index = graphDb.index().forNodes("email");
			Index<Node> user_weight_index = graphDb.index().forNodes("user_weight");
			
			Node user_node = userId_index.get("id",user_name).getSingle();
			if(user_node != null)
				throw new RuntimeException("Unbale to create new user, User node already exists for user_name "+user_name+".");
			
			user_node = user(first_name,last_name,user_name,email,location,previlege,time_created,weight,0,"","D",time_created,time_created,"",acc_type,"");  // Creating a new user node
			//Indexing newly created user node
			userId_index.add(user_node, "id", user_name);
			first_name_index.add(user_node,"id",first_name);
			email_index.add(user_node,"id",email);
			user_weight_index.add(user_node,"weight",new ValueContext(weight).indexNumeric());
			
			Index<Node> UserTilesIndex = graphDb.index().forNodes("user_tiles");  // get user tiles index
			Node tiles_node = UserTilesIndex.get("id",user_name).getSingle();
			if(tiles_node != null)
		       UserTilesIndex.remove(tiles_node);
			tiles_node = user_tiles(user_name,"","");  // creating new user tile node to store the personalized tiles of the user
			UserTilesIndex.add(tiles_node,"id",user_name); //indexing the user tiles node
			
			//Linking user to a location(state)
			String loc = location.toLowerCase();
			if(!loc.equals(""))
	        {
	           Index<Node> location_index = graphDb.index().forNodes("location");
	           boolean loc_not_found = false;
	           for(Node state: location_index.query( "id", "*" ))
	        	 if(loc_not_found && !"all".equalsIgnoreCase(state.getProperty("location_id").toString()))
	        		for(String city: state.getProperty("cities").toString().split(","))	        			
	        			if(city.equalsIgnoreCase(location)){
	        				user_node.createRelationshipTo(state, DynamicRelationshipType.withName("Belongs_To_Location")); // breaking the loop when the user is linked to his location(state)
	        				loc_not_found = false;
	        				break;
	        			}
	        }
		    ret = true;  // changing the return value to TRUE when the user is created successfully
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ create_user('"+first_name+"','"+last_name+"','"+user_name+"','"+email+"','"+location+"',"+previlege+","+time_created+","+weight+",'"+acc_type+"')");
			System.out.println("Something went wrong, while creating user from create_user  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public boolean user_subscribefeed(String user_name, String feed_type)
			throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> userId_index = graphDb.index().forNodes("user");
			Node user_node = userId_index.get("id",user_name).getSingle();
			if(user_node == null)
				throw new RuntimeException("Unbale to get the user details, User node not found for the given user_name "+user_name+".");
			user_node.setProperty("feed_subscription", feed_type);		
		    ret = true;  // changing the return value to TRUE when the update is completed
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ user_subscribefeed('"+user_name+"','"+feed_type+"')");
			System.out.println("Something went wrong, while updating user sbscribed feeed from user_subscribefeed()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public boolean edit_user(String first_name, String last_name,
			String user_name, String email, String location, int previlege,
			String acc_type) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> userId_index = graphDb.index().forNodes("user");
			Index<Node> first_name_index = graphDb.index().forNodes("firstname");
			Index<Node> email_index = graphDb.index().forNodes("email");
			
			Node user_node = userId_index.get("id",user_name).getSingle();
			if(user_node == null)
				throw new RuntimeException("Unbale to get the user details, User node not found for the given user_name "+user_name+".");
			
			user_node.setProperty("first_name",first_name);
		    user_node.setProperty("last_name",last_name);
		    user_node.setProperty("email",email);
		    user_node.setProperty("location",location);
		    user_node.setProperty("previlege",previlege);
		    user_node.setProperty("acc_type",acc_type);
			
			first_name_index.remove(user_node);
			email_index.remove(user_node);
			first_name_index.add(user_node,"id",first_name);
			email_index.add(user_node,"id",email);
			//remove relationship with location
			if(user_node.hasRelationship(Direction.OUTGOING,DynamicRelationshipType.withName("Belongs_To_Location")))
			{
				Relationship rel = user_node.getSingleRelationship(DynamicRelationshipType.withName("Belongs_To_Location"),Direction.OUTGOING);
				rel.delete();
			}
			
			//Linking user to a location(state)
			String loc = location.toLowerCase();
			if(!loc.equals(""))
	        {
	           Index<Node> location_index = graphDb.index().forNodes("location");
	           boolean loc_not_found = false;
	           for(Node state: location_index.query( "id", "*" ))
	        	 if(loc_not_found && !"all".equalsIgnoreCase(state.getProperty("location_id").toString()))
	        		for(String city: state.getProperty("cities").toString().split(","))	        			
	        			if(city.equalsIgnoreCase(location)){
	        				user_node.createRelationshipTo(state, DynamicRelationshipType.withName("Belongs_To_Location")); // breaking the loop when the user is linked to his location(state)
	        				loc_not_found = false;
	        				break;
	        			}
	        }
		    ret = true;  // changing the return value to TRUE when the user is successfully updated
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ edit_user('"+first_name+"','"+last_name+"','"+user_name+"','"+email+"','"+location+"',"+previlege+",'"+acc_type+"')");
			System.out.println("Something went wrong, while updating user from edit_user  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public boolean update_weight(String item_type, String item_id, int weight)
			throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			String index_name = indexFullName.get(item_type);
			String weight_index_name = index_name + "_weight";
			
			Index<Node> index = graphDb.index().forNodes(index_name);
			Index<Node> weight_index = graphDb.index().forNodes(weight_index_name);
			
			Node item = index.get("id",item_id).getSingle();
			if(item == null)
				throw new RuntimeException("Unbale to get the item details, Item not found for the given id "+item_id+".");
			
			if(item_type.toLowerCase().equals("a"))
			{
				for(Relationship each: item.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("article_voteup")))
					each.setProperty("in_weight", Math.round(((float)weight)/100));
				for(Relationship each: item.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("article_markfav")))
					each.setProperty("in_weight", Math.round(((float)weight)/100));		
			}
			
			item.setProperty("weight", weight);
			weight_index.remove(item);
			weight_index.add(item, "weight", weight);
		    
		    ret = true;  // changing the return value to TRUE when the user is successfully updated
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ update_weight('"+item_type+"','"+item_id+"',"+weight+")");
			System.out.println("Something went wrong, while updating weight from update_weight  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public boolean add_friends(String user_name, String f_type, String f_ids)
			throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> userIndex = graphDb.index().forNodes("user");
			Node userNode = userIndex.get("id",user_name).getSingle();
			if(userNode == null)
				throw new RuntimeException("Unbale to get the user details, User not found for the given user name "+user_name+".");
			
			JSONArray friendsJSONArray;
			
			if(userNode.hasProperty("friends"))
			{
				String friends = userNode.getProperty("friends").toString();
				friendsJSONArray = new JSONArray(friends);
			}
			else
			{
				friendsJSONArray = new JSONArray();
				friendsJSONArray.put(0,"");
				friendsJSONArray.put(1,"");
				friendsJSONArray.put(2,"");
			}

			if(f_type.toLowerCase().equals("f"))
				friendsJSONArray.put(0,f_ids);
			else if(f_type.toLowerCase().equals("t"))
				friendsJSONArray.put(1,f_ids);
			else
				friendsJSONArray.put(2,f_ids);
			String jsonArrayString = friendsJSONArray.toString(); 
			userNode.setProperty("friends",jsonArrayString);
			
		    
		    ret = true;  // changing the return value to TRUE when the user is successfully updated
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ add_friends('"+user_name+"','"+f_type+"','"+f_ids+"')");
			System.out.println("Something went wrong, while updating friends list from add_friends  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public boolean skip_from_special_tiles(String item_id, String item_type,
			int skip) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			if("A".equalsIgnoreCase(item_type))
			{
				Index<Node> article_index = graphDb.index().forNodes("article");
				Node article = article_index.get("id", item_id).getSingle();
				if(article != null)
					article.setProperty("skip_from_special_tiles", skip);
				else throw new RuntimeException("Article node not found for the given article id : "+item_id);
				
			}else if("E".equalsIgnoreCase(item_type))
			{
				Index<Node> event_index = graphDb.index().forNodes("event");
				Node event = event_index.get("id", item_id).getSingle();
				if(event != null)
					event.setProperty("skip_from_special_tiles", skip);
				else throw new RuntimeException("Event node not found for the given event id : "+item_id);
				
			}else if("P".equalsIgnoreCase(item_type))
			{
				Index<Node> petition_index = graphDb.index().forNodes("petition");
				Node petition = petition_index.get("id", item_id).getSingle();
				if(petition != null)
					petition.setProperty("skip_from_special_tiles", skip);
				else throw new RuntimeException("Petition node not found for the given petition id : "+item_id);
				
			}else if("D".equalsIgnoreCase(item_type))
			{
				Index<Node> debate_index = graphDb.index().forNodes("debate");
				Node debate = debate_index.get("id", item_id).getSingle();
				if(debate != null)
					debate.setProperty("skip_from_special_tiles", skip);
				else throw new RuntimeException("Debate node not found for the given debate id : "+item_id);
				
			}else if("T".equalsIgnoreCase(item_type))
			{
				Index<Node> townhall_index = graphDb.index().forNodes("townhall");
				Node townhall = townhall_index.get("id", item_id).getSingle();
				if(townhall != null)
					townhall.setProperty("skip_from_special_tiles", skip);
				else throw new RuntimeException("Townhall node not found for the given townhall id : "+item_id);
				
			}
			else throw new RuntimeException("Invalid value found for the parameter item_type: "+item_type);
			
			//update the list of decommisioned tiles list
			Index<Node> decommisioned_tiles_index = graphDb.index().forNodes("decommissioned_tiles");
			Node list_node = decommisioned_tiles_index.get("id","skip_items").getSingle();
			if(list_node == null && skip == 1)
			{
				Node tiles_node = tiles("skip_items",item_id);
				decommisioned_tiles_index.add(tiles_node,"id","skip_items");
			}else{
				String listItems = list_node.getProperty("value").toString().replaceAll(item_id,"").replaceAll(",,", ",");
				if(skip == 1)
					listItems = listItems.concat(","+item_id);
				list_node.setProperty("value", listItems);
			}
			
			calc_views();
			update_tiles_temp();
			update_tiles_td();
			
			ret = true;  // changing the return value to TRUE when the update is completed
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ skip_from_special_tiles('"+item_id+"','"+item_type+"',"+skip+")");
			System.out.println("Something went wrong, while updating decommisioned tiles from skip_from_special_tiles()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public boolean update_item_lang(String item_id, String item_type,
			String item_lang) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			if("A".equalsIgnoreCase(item_type))
			{
				Index<Node> article_index = graphDb.index().forNodes("article");
				Node article = article_index.get("id", item_id).getSingle();
				if(article != null)
					article.setProperty("lang", item_lang);
				else throw new RuntimeException("Article node not found for the given article id : "+item_id);
				
			}else if("E".equalsIgnoreCase(item_type))
			{
				Index<Node> event_index = graphDb.index().forNodes("event");
				Node event = event_index.get("id", item_id).getSingle();
				if(event != null)
					event.setProperty("lang", item_lang);
				else throw new RuntimeException("Event node not found for the given event id : "+item_id);
				
			}else if("P".equalsIgnoreCase(item_type))
			{
				Index<Node> petition_index = graphDb.index().forNodes("petition");
				Node petition = petition_index.get("id", item_id).getSingle();
				if(petition != null)
					petition.setProperty("lang", item_lang);
				else throw new RuntimeException("Petition node not found for the given petition id : "+item_id);
				
			}else if("D".equalsIgnoreCase(item_type))
			{
				Index<Node> debate_index = graphDb.index().forNodes("debate");
				Node debate = debate_index.get("id", item_id).getSingle();
				if(debate != null)
					debate.setProperty("lang", item_lang);
				else throw new RuntimeException("Debate node not found for the given debate id : "+item_id);
				
			}else if("T".equalsIgnoreCase(item_type))
			{
				Index<Node> townhall_index = graphDb.index().forNodes("townhall");
				Node townhall = townhall_index.get("id", item_id).getSingle();
				if(townhall != null)
					townhall.setProperty("lang", item_lang);
				else throw new RuntimeException("Townhall node not found for the given townhall id : "+item_id);
				
			}
			else throw new RuntimeException("Invalid value found for the parameter item_type: "+item_type);
			
			ret = true;  // changing the return value to TRUE when the update is completed
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ update_item_lang('"+item_id+"','"+item_type+"','"+item_lang+"')");
			System.out.println("Something went wrong, while updating item language from update_item_lang()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public String get_friends(String user_name) throws TException {
		// TODO Auto-generated method stub something

		boolean ret = false;
		JSONArray resJSON = new JSONArray();
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> user_index = graphDb.index().forNodes("user");
//			Index<Node> firstname_index = graphDb.index().forNodes("firstname");
//			Index<Node> email_index = graphDb.index().forNodes("email");
			
			Node user_node = user_index.get("id", user_name).getSingle();
			
			if(user_node != null && user_node.hasProperty("friends"))
			{
//				IndexHits<Node> total_users = user_index.query("id", "*");
//				IndexHits<Node> fname_users = firstname_index.query("id", "*");
//				IndexHits<Node> email_users = email_index.query("id", "*");
				
//				String friends_list = user_node.getProperty("friends").toString();
				// TODO Auto-generated method stub do it later

			}
			ret = true;  // changing the return value to TRUE when the update is completed
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_friends('"+user_name+"')");
			System.out.println("Something went wrong, while getting friends list from get_friends()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		if(ret == false)
			return "";
		else
			return resJSON.toString();
	
	}

	public String get_follow_suggestions(String user_name, int count,
			int prev_cnt) throws TException {
		// TODO Auto-generated method stub something
		
		boolean ret = false;
		JSONArray resJSON = new JSONArray();
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> user_index = graphDb.index().forNodes("user");
//			Index<Node> firstname_index = graphDb.index().forNodes("firstname");
//			Index<Node> email_index = graphDb.index().forNodes("email");
			
			Node user_node = user_index.get("id", user_name).getSingle();
			
			if(user_node != null && user_node.hasProperty("friends"))
			{
//				IndexHits<Node> total_users = user_index.query("id", "*");
//				IndexHits<Node> fname_users = firstname_index.query("id", "*");
//				IndexHits<Node> email_users = email_index.query("id", "*");
//				
//				String friends_list = user_node.getProperty("friends").toString();
				// TODO Auto-generated method stub do it later
			}
			ret = true;  // changing the return value to TRUE when the update is completed
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_friends('"+user_name+"')");
			System.out.println("Something went wrong, while getting friends list from get_friends()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		if(ret == false)
			return "";
		else
			return resJSON.toString();
	
	}

	public String get_articles_hashtag(String user_name, String hash,
			int count, int prev_cnt) throws TException {
		boolean ret = false;
		JSONArray resJSON = new JSONArray();
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> hashtag_index = graphDb.index().forNodes("sub_category");
			Index<Node> user_index = graphDb.index().forNodes("user");

			Node hashtag_node = hashtag_index.get("name",hash).getSingle();
			Node user_node = user_index.get("id",user_name).getSingle();
			
			if(hashtag_node == null)
				throw new RuntimeException("Unbale to get the hashtag details, Hashtag not found for the given hash "+hash+".");
			
			ArrayList<Node> res = new ArrayList<Node>();
			
			for(Relationship each: hashtag_node.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("Belongs_To_Subcategory_Article")))
			{
				Node item_node = each.getOtherNode(hashtag_node);
				if(Integer.parseInt(item_node.getProperty("space").toString())==0 && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
					res.add(item_node);
			}
			for(Relationship each: hashtag_node.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("Belongs_To_Subcategory_Event")))
			{
				Node item_node = each.getOtherNode(hashtag_node);
				if(Integer.parseInt(item_node.getProperty("space").toString())==0 && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
					res.add(item_node);
			}
			for(Relationship each: hashtag_node.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("Belongs_To_Subcategory_Petition")))
			{
				Node item_node = each.getOtherNode(hashtag_node);
				if(Integer.parseInt(item_node.getProperty("space").toString())==0 && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Petition_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
					res.add(item_node);
			}
			for(Relationship each: hashtag_node.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("Belongs_To_Subcategory_Debate")))
			{
				Node item_node = each.getOtherNode(hashtag_node);
				if(Integer.parseInt(item_node.getProperty("space").toString())==0 && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Debate_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
					res.add(item_node);
			}
			for(Relationship each: hashtag_node.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("Belongs_To_Subcategory_Townhall")))
			{
				Node item_node = each.getOtherNode(hashtag_node);
				if(Integer.parseInt(item_node.getProperty("space").toString())==0 && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Townhall_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
					res.add(item_node);
			}

			res.trimToSize();
			Collections.sort(res, TimeCreatedComparatorForNodes);
			int totalLen = res.size();
			int fromIndex = prev_cnt;
			int toIndex = totalLen < count + prev_cnt ? totalLen : count+prev_cnt;
			if(fromIndex > toIndex)
				toIndex = fromIndex;
			
			List<Node> actualRes = res.subList(fromIndex, toIndex);
			
			for(Node item: actualRes)
			{
				if(item.getProperty("__CLASS__").toString().equals("Saddahaq.article"))
					resJSON.put(getArticleJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.event"))
					resJSON.put(getEventJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.petition"))
					resJSON.put(getPetitionJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.townhall"))
					resJSON.put(getTownhallJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.debate"))
					resJSON.put(getDebateJSONForTile(item, false, user_node));
			}
		    
		    ret = true;  // changing the return value to TRUE if everything is fine
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_articles_hashtag('"+user_name+"','"+hash+"',"+count+","+prev_cnt+")");
			System.out.println("Something went wrong, while reading items from get_articles_hashtag  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		if(ret == false)
			return "";
		else
			return resJSON.toString();

	}

	public String get_articles_space(String user_name, String space, int count,
			int prev_cnt, int admin_tagged) throws TException {
		boolean ret = false;
		JSONArray resJSON = new JSONArray();
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> space_index = graphDb.index().forNodes("space");
			Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> article_index = graphDb.index().forNodes("article");
			Index<Node> event_index = graphDb.index().forNodes("event");
			Index<Node> petition_index = graphDb.index().forNodes("petition");
			Index<Node> townhall_index = graphDb.index().forNodes("townhall");
			Index<Node> debate_index = graphDb.index().forNodes("debate");

			Node space_node = space_index.get("id",space).getSingle();
			Node user_node = user_index.get("id",user_name).getSingle();
			
			if(space_node == null)
				throw new RuntimeException("Unbale to get the space details, Space not found for the given id "+space+".");
			
			ArrayList<Node> res = new ArrayList<Node>();
			
			for(Relationship each: space_node.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("Article_Tagged_To_Space")))
			{
				Node item_node = each.getOtherNode(space_node);
				if(admin_tagged == 0)
				{
					if(!each.hasProperty("admin_tagged") && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
				}
				else if(admin_tagged == 1)
				{
					if(each.hasProperty("admin_tagged") && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
				}
				else if(Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
			}
			for(Relationship each: space_node.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("Event_Tagged_To_Space")))
			{
				Node item_node = each.getOtherNode(space_node);
				if(admin_tagged == 0)
				{
					if(!each.hasProperty("admin_tagged") && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
				}
				else if(admin_tagged == 1)
				{
					if(each.hasProperty("admin_tagged") && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
				}
				else if(Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
			}
			for(Relationship each: space_node.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("Petition_Tagged_To_Space")))
			{
				Node item_node = each.getOtherNode(space_node);
				if(admin_tagged == 0)
				{
					if(!each.hasProperty("admin_tagged") && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Petition_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
				}
				else if(admin_tagged == 1)
				{
					if(each.hasProperty("admin_tagged") && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Petition_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
				}
				else if(Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Petition_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
			}
			for(Relationship each: space_node.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("Debate_Tagged_To_Space")))
			{
				Node item_node = each.getOtherNode(space_node);
				if(admin_tagged == 0)
				{
					if( !each.hasProperty("admin_tagged") && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Debate_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
				}
				else if(admin_tagged == 1)
				{
					if( each.hasProperty("admin_tagged") && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Debate_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
				}
				else if(Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Debate_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
			}
			for(Relationship each: space_node.getRelationships(Direction.BOTH, DynamicRelationshipType.withName("Townhall_Tagged_To_Space")))
			{
				Node item_node = each.getOtherNode(space_node);
				if(admin_tagged == 0)
				{
					if(!each.hasProperty("admin_tagged") && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Townhall_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
				}
				else if(admin_tagged == 1)
				{
					if(each.hasProperty("admin_tagged") && Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Townhall_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
				}
				else if(Integer.parseInt(item_node.getProperty("skip_from_special_tiles").toString())==0 && Integer.parseInt(item_node.getSingleRelationship(DynamicRelationshipType.withName("Townhall_Written_By"),Direction.OUTGOING).getOtherNode(item_node).getProperty("acc_type").toString())!=3)
						res.add(item_node);
			}

			res.trimToSize();
			Collections.sort(res, TimeCreatedComparatorForNodes);
			
			ArrayList<Node> totalItems = new ArrayList<Node>();
			
			String pin_tiles_string = space_node.getProperty("pins").toString();
			for(String pinItemId: pin_tiles_string.split(","))
			{
				if(totalItems.size() == 3)
					break;
				if(article_index.get("id",pinItemId).getSingle() != null)
					totalItems.add(article_index.get("id",pinItemId).getSingle());
				else if(event_index.get("id",pinItemId).getSingle() != null)
					totalItems.add(event_index.get("id",pinItemId).getSingle());
				else if(petition_index.get("id",pinItemId).getSingle() != null)
					totalItems.add(petition_index.get("id",pinItemId).getSingle());
				else if(debate_index.get("id",pinItemId).getSingle() != null)
					totalItems.add(debate_index.get("id",pinItemId).getSingle());
				else if(townhall_index.get("id",pinItemId).getSingle() != null)
					totalItems.add(townhall_index.get("id",pinItemId).getSingle());
			}
			
			res.removeAll(totalItems);
			
			totalItems.addAll(res);

			int totalLen = totalItems.size();
			int fromIndex = prev_cnt;
			int toIndex = totalLen < count + prev_cnt ? totalLen : count+prev_cnt;
			if(fromIndex > toIndex)
				toIndex = fromIndex;
			
			List<Node> actualRes = totalItems.subList(fromIndex, toIndex);
			
			
			for(Node item: actualRes)
			{
				if(item.getProperty("__CLASS__").toString().equals("Saddahaq.article"))
					resJSON.put(getArticleJSONForTile(item, pin_tiles_string.contains(item.getProperty("article_id").toString()), user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.event"))
					resJSON.put(getEventJSONForTile(item, pin_tiles_string.contains(item.getProperty("event_id").toString()), user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.petition"))
					resJSON.put(getPetitionJSONForTile(item, pin_tiles_string.contains(item.getProperty("p_id").toString()), user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.townhall"))
					resJSON.put(getTownhallJSONForTile(item, pin_tiles_string.contains(item.getProperty("t_id").toString()), user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.debate"))
					resJSON.put(getDebateJSONForTile(item, pin_tiles_string.contains(item.getProperty("d_id").toString()), user_node));
			}
		    
		    ret = true;  // changing the return value to TRUE if everything is fine
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_articles_space('"+user_name+"','"+space+"',"+count+","+prev_cnt+","+admin_tagged+")");
			System.out.println("Something went wrong, while reading items from get_articles_space  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		if(ret == false)
			return "";
		else
			return resJSON.toString();

	}

	public String action_performed_users(String item_type, String item_id,
			String action_type, int count, int prev_cnt) throws TException {
		boolean ret = false;
		JSONArray resJSON = new JSONArray();
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> item_index = graphDb.index().forNodes(indexFullName.get(item_type));
			Node item_node = item_index.get("id", item_id).getSingle();
			
			int counter = 0;
			
			if(item_node == null)
				throw new RuntimeException("No item found for the given item type:"+item_type+" and item id:"+item_id);
			
			ArrayList<Node> total_users = new ArrayList<Node>();
			
			if("a".equalsIgnoreCase(item_type))
			{
				if("rl".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("article_readlater")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
				else if("vu".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("article_voteup")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
				else if("mark_fav".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("article_markfav")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
				else if("cmnt".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Comment_To_Article")))
					{
						Node tmp_user = rel.getOtherNode(item_node).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"),Direction.OUTGOING).getEndNode();
						if(total_users.contains(tmp_user))continue;
						else if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(tmp_user);
					}
	   		  	}
				else if("view".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Viewed_By")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}	
			}
			else if("e".equalsIgnoreCase(item_type))
			{
				if("attend".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Is_Attending")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
				else if("rl".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("event_readlater")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
				else if("cmnt".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Comment_To_Event")))
					{
						Node tmp_user = rel.getOtherNode(item_node).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"),Direction.OUTGOING).getEndNode();
						if(total_users.contains(tmp_user))continue;
						else if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(tmp_user);
					}
	   		  	}
				else if("view".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Viewed_By")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}	
			}
			else if("p".equalsIgnoreCase(item_type))
			{
				if("sign".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Signed_Petition")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
				else if("cmnt".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Comment_To_Petition")))
					{
						Node tmp_user = rel.getOtherNode(item_node).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"),Direction.OUTGOING).getEndNode();
						if(total_users.contains(tmp_user))continue;
						else if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(tmp_user);
					}
	   		  	}
				else if("view".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Viewed_By")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
			}
			else if("t".equalsIgnoreCase(item_type))
			{
				if("asked_ques".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Asked_Question")))
					{
						if(total_users.contains(rel.getOtherNode(item_node)))continue;
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
				else if("cmnt".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Commented_On-Townhall")))
					{
						Node tmp_user = rel.getOtherNode(item_node).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"),Direction.OUTGOING).getEndNode();
						if(total_users.contains(tmp_user))continue;
						else if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(tmp_user);
					}
	   		  	}
				else if("view".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Viewed_By")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
			}
			else if("d".equalsIgnoreCase(item_type))
			{
				if("asked_ques".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Asked_Debate_Question")))
					{
						if(total_users.contains(rel.getOtherNode(item_node)))continue;
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
				else if("cmnt".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Commented_On_Debate")))
					{
						Node tmp_user = rel.getOtherNode(item_node).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"),Direction.OUTGOING).getEndNode();
						if(total_users.contains(tmp_user))continue;
						else if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(tmp_user);
					}
	   		  	}
				else if("view".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Viewed_By")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}	
			}
			else if("s".equalsIgnoreCase(item_type))
			{
				if("admin".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Admin_Of_Space")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}
				else if("follow".equalsIgnoreCase(action_type))
	   		  	{
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Space_Followed_By")))
					{
						if(counter < prev_cnt){counter++; continue;}
						else if(counter >= count+prev_cnt) break;
						total_users.add(rel.getOtherNode(item_node));
					}
	   		  	}	
			}
			
			for(Node eachUser: total_users)
			{
				JSONObject eachUserJSON = new JSONObject();
				eachUserJSON.put("uname", eachUser.getProperty("user_name").toString());
				eachUserJSON.put("ust", Integer.parseInt(eachUser.getProperty("acc_type").toString()));
				eachUserJSON.put("fullname", eachUser.getProperty("first_name").toString() + " " + eachUser.getProperty("last_name").toString());
				eachUserJSON.put("wt",Integer.parseInt(eachUser.getProperty("weight").toString()));
				resJSON.put(eachUserJSON);
			}
			
			ret = true;  // changing the return value to TRUE when the update is completed
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ action_performed_users('"+item_type+"','"+item_id+"','"+action_type+"',"+count+","+prev_cnt+")");
			System.out.println("Something went wrong, while getting list of users from action_performed_users()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		if(ret == false)
			return "";
		else
			return resJSON.toString();
	
	}

	public String unread_notification_count(String user_name) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public boolean update_user_status(String user_name, String acc_type)
			throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> user_index = graphDb.index().forNodes("user");
			Node userNode = user_index.get("id", user_name).getSingle();
			if(userNode == null)
				throw new RuntimeException("No user found for the given user_name:"+user_name);
			String existingAccType = userNode.getProperty("acc_type").toString();
			userNode.setProperty("acc_type", acc_type);
			if("3".equals(existingAccType) || "3".equals(acc_type) ){
				calc_views();
				update_tiles_temp();
				update_tiles_td();
			}
			
			ret = true;  // changing the return value to TRUE when the update is completed
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ update_user_status('"+user_name+"','"+acc_type+"')");
			System.out.println("Something went wrong, while updating user status from update_user_status()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;
	}

	public String get_calc_timings() throws TException {
		String ret = "";
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> trending_calc_times = graphDb.index().forNodes("trending_calc_times");
			Node timingNode = trending_calc_times.get("id", "all").getSingle();
			if(timingNode == null)
				throw new RuntimeException("No timing node found at trending_calc_times index for id:all");
			ret = timingNode.getProperty("timing_list").toString();
			
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_calc_timings()");
			System.out.println("Something went wrong, while reading calc timings from get_calc_timings()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = "";
		}
		finally{
			
		}
		return ret;
	}

	public String get_item_stats_weights(String item_type, String item_id)
			throws TException {
		boolean ret = false;
		JSONObject resJSON = new JSONObject();
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> item_index = graphDb.index().forNodes(indexFullName.get(item_type));
			Node item_node = item_index.get("id", item_id).getSingle();
						
			if(item_node == null)
				throw new RuntimeException("No item found for the given item type:"+item_type+" and item id:"+item_id);
			
			if(item_node.hasRelationship(DynamicRelationshipType.withName("item_trending_weights")))
			{
				Node weights_node = item_node.getSingleRelationship(DynamicRelationshipType.withName("item_trending_weights"), Direction.OUTGOING).getEndNode();
				
				if("a".equalsIgnoreCase(item_type))
	    	    {
					resJSON.put("title",item_node.getProperty("article_title").toString());
					resJSON.put("id",item_node.getProperty("article_id").toString());
				}
				else if("e".equalsIgnoreCase(item_type))
				{
					resJSON.put("title",item_node.getProperty("event_title").toString());
					resJSON.put("id",item_node.getProperty("event_id").toString());
				}
				else if("p".equalsIgnoreCase(item_type))
				{
					resJSON.put("title",item_node.getProperty("p_title").toString());
					resJSON.put("id",item_node.getProperty("p_id").toString());
				}
				else if("d".equalsIgnoreCase(item_type))
				{
					resJSON.put("title",item_node.getProperty("d_title").toString());
					resJSON.put("id",item_node.getProperty("d_id").toString());
				}
				else if("t".equalsIgnoreCase(item_type))
				{
					resJSON.put("title",item_node.getProperty("t_title").toString());
					resJSON.put("id",item_node.getProperty("t_id").toString());
				}

				resJSON.put("weights",weights_node.getProperty("weights").toString());
				resJSON.put("zscore",weights_node.getProperty("zscore").toString());
				resJSON.put("stddev",weights_node.getProperty("stddev").toString());
				resJSON.put("stddev_change",weights_node.getProperty("stddev_change").toString());
			}
			
			tx.success();
			ret = true;  // changing the return value to TRUE when the update is completed
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_item_stats_weights('"+item_type+"','"+item_id+"')");
			System.out.println("Something went wrong, while getting list of users from get_item_stats_weights()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		if(ret == false)
			return "";
		else
			return resJSON.toString();
	}

	public boolean delete_user(String user_name) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> firstname_index = graphDb.index().forNodes("firstname");
			Index<Node> email_index = graphDb.index().forNodes("email");
			Index<Node> user_weight_index = graphDb.index().forNodes("user_weight");
			Index<Node> UserTilesIndex = graphDb.index().forNodes("user_tiles");
			IndexHits<Node> users = user_index.query("id", user_name);
			while(users.hasNext())
			{
				Node user = users.next();
				for(Relationship rel : user.getRelationships())
				{
					if(rel.getType().toString().equals("Article_Written_By"))
						delete_article(rel.getOtherNode(user).getProperty("article_id").toString());
					else if(rel.getType().toString().equals("Event_Created_By"))
						delete_article(rel.getOtherNode(user).getProperty("event_id").toString());
					else if(rel.getType().toString().equals("Quickpost_Written_By"))
						delete_article(rel.getOtherNode(user).getProperty("qp_id").toString());
					else if(rel.getType().toString().equals("Petition_Written_By"))
						delete_article(rel.getOtherNode(user).getProperty("p_id").toString());
					else if(rel.getType().toString().equals("Debate_Written_By"))
						delete_article(rel.getOtherNode(user).getProperty("d_id").toString());
					else if(rel.getType().toString().equals("Townhall_Written_By"))
						delete_article(rel.getOtherNode(user).getProperty("t_id").toString());
					else if(rel.getType().toString().equals("Debate_Suggestion_Written_By"))
						delete_debate_townhall_suggestion("D",rel.getOtherNode(user).getProperty("d_id").toString());
					else if(rel.getType().toString().equals("Townhall_Suggestion_Written_By"))
						delete_debate_townhall_suggestion("T",rel.getOtherNode(user).getProperty("t_id").toString());
					else if(rel.getType().toString().equals("Comment_Written_By"))
						delete_comment(rel.getOtherNode(user).getProperty("comment_id").toString());
					else if(rel.getType().toString().equals("Space_Created_By"))
						delete_space(rel.getOtherNode(user).getProperty("space_id").toString());
					else
						rel.delete();
				}
				
				user_index.remove(user);
				firstname_index.remove(user);
				email_index.remove(user);
				user_weight_index.remove(user);
				
				IndexHits<Node> user_tiles_node = UserTilesIndex.query("id",user_name);
				while(user_tiles_node.hasNext())
			    {
					Node node = user_tiles_node.next();
					UserTilesIndex.remove(node);
					node.delete();
			    }
				
				user.delete();
				ret = true;  // changing the return value to TRUE when the update is completed
			}
			
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ delete_user('"+user_name+"')");
			System.out.println("Something went wrong, while deleting user from delete_user()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;
	}

	public boolean user_follow(String user_name1, String user_name2, int time)
			throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> user_weight_index = graphDb.index().forNodes("user_weight");
			
			int curTime = (int)(System.currentTimeMillis()/1000) - (86400*2);
			Node user1 = user_index.get("id", user_name1).getSingle();
			Node user2 = user_index.get("id", user_name2).getSingle();

			if(user1 == null)
				throw new RuntimeException("User not found for the given user name:"+user_name1);
			if(user2 == null)
				throw new RuntimeException("User not found for the given user name:"+user_name2);
			
			for(Relationship rel: user1.getRelationships(DynamicRelationshipType.withName("Follows"),Direction.OUTGOING))
				if(rel.getEndNode().equals(user2))
					throw new RuntimeException(user_name1 + " is already following " + user_name2);
			
			user1.setProperty("last_seen", curTime);
			int u1_wt = Integer.parseInt(user1.getProperty("weight").toString());
			int u2_wt = Integer.parseInt(user2.getProperty("weight").toString());
			
			u2_wt = u2_wt + Math.round(((float)u1_wt)/100);
			user2.setProperty("weight",u2_wt);			
			Relationship followRel = user1.createRelationshipTo(user2, DynamicRelationshipType.withName("Follows"));
			followRel.setProperty("time", curTime);
			followRel.setProperty("in_weight", Math.round(((float)u1_wt)/100));
			followRel.setProperty("out_weight", 0);
			
			user_weight_index.remove(user2);
			user_weight_index.add(user2, "weight", new ValueContext(u2_wt).indexNumeric());
			
			calc_user_tiles(user_name1);
				
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ user_follow('"+user_name1+"','"+user_name2+"',"+time+")");
			System.out.println("Something went wrong, while follwing user from user_follow()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;
	}

	public boolean space_follow(String user_name, String id) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> space_index = graphDb.index().forNodes("space");

			Node user = user_index.get("id", user_name).getSingle();
			Node space = space_index.get("id", id).getSingle();
			
			int curTime = (int)(System.currentTimeMillis()/1000) - (86400*2);

			if(user == null)
				throw new RuntimeException("User not found for the given user name:"+user_name);
			if(space == null)
				throw new RuntimeException("Space not found for the given space id:"+id);
			
			boolean isAlreadyFollowing = false; 
			
			for(Relationship rel: space.getRelationships(DynamicRelationshipType.withName("Space_Followed_By"),Direction.OUTGOING))
				if(rel.getEndNode().equals(user))
				{
					rel.delete();
					isAlreadyFollowing = true;
					break;
				}
			
			if(isAlreadyFollowing == false)
			{
				Relationship followRel = space.createRelationshipTo(user, DynamicRelationshipType.withName("Space_Followed_By"));
				followRel.setProperty("time", curTime);				
			}
				
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ space_follow('"+user_name+"','"+id+"')");
			System.out.println("Something went wrong, while follwing space from space_follow()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;
	}

	public boolean space_admin(String user_name, String space_id)
			throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> space_index = graphDb.index().forNodes("space");

			Node user = user_index.get("id", user_name).getSingle();
			Node space = space_index.get("id", space_id).getSingle();
			
			int curTime = (int)(System.currentTimeMillis()/1000) - (86400*2);

			if(user == null)
				throw new RuntimeException("User not found for the given user name:"+user_name);
			if(space == null)
				throw new RuntimeException("Space not found for the given space id:"+space_id);
			
			boolean isAlreadyAdmin = false; 
			
			for(Relationship rel: space.getRelationships(DynamicRelationshipType.withName("Admin_Of_Space"),Direction.OUTGOING))
				if(rel.getEndNode().equals(user))
				{
					rel.delete();
					isAlreadyAdmin = true;
					break;
				}
			
			if(isAlreadyAdmin == false)
			{
				Relationship followRel = space.createRelationshipTo(user, DynamicRelationshipType.withName("Admin_Of_Space"));
				followRel.setProperty("time", curTime);				
			}
				
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ space_admin('"+user_name+"','"+space_id+"')");
			System.out.println("Something went wrong, while modifying space admin from space_admin()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;
	}

	public boolean space_tagitem(String space_id, String item_type,
			String item_id, String tag_type) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> space_index = graphDb.index().forNodes("space");

			Node space = space_index.get("id", space_id).getSingle();
			Node item = null;
			String relationName = "";
			
			if("a".equalsIgnoreCase(item_type))
			{
				item = graphDb.index().forNodes("article").get("id", item_id).getSingle();
				relationName = "Article_Tagged_To_Space";
			}
			else if("e".equalsIgnoreCase(item_type))
			{
				item = graphDb.index().forNodes("event").get("id", item_id).getSingle();
				relationName = "Event_Tagged_To_Space";
			}
			else if("p".equalsIgnoreCase(item_type))
			{
				item = graphDb.index().forNodes("petition").get("id", item_id).getSingle();
				relationName = "Petition_Tagged_To_Space";
			}
			else if("t".equalsIgnoreCase(item_type))
			{
				item = graphDb.index().forNodes("townhall").get("id", item_id).getSingle();
				relationName = "Townhall_Tagged_To_Space";
			}
			else if("d".equalsIgnoreCase(item_type))
			{
				item = graphDb.index().forNodes("debate").get("id", item_id).getSingle();
				relationName = "Debate_Tagged_To_Space";
			}
			
			
			int curTime = (int)(System.currentTimeMillis()/1000) - (86400*2);

			if(item == null)
				throw new RuntimeException("Item not found for the given item type:"+item_type+" and item_id:"+item_id);
			if(space == null)
				throw new RuntimeException("Space not found for the given space id:"+space_id);
			
			boolean isAlreadyTagged = false; 
			
			for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName(relationName),Direction.BOTH))
				if(rel.getOtherNode(item).equals(space))
				{
					rel.delete();
					isAlreadyTagged = true;
					break;
				}
			
			if(isAlreadyTagged == false)
			{
				Relationship followRel = item.createRelationshipTo(space, DynamicRelationshipType.withName(relationName));
				followRel.setProperty("time", curTime);		
				if(tag_type.equals("A"))
					followRel.setProperty("admin_tagged", 1);
			}	
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ space_tagitem('"+space_id+"','"+item_type+"','"+item_id+"','"+tag_type+"')");
			System.out.println("Something went wrong, while tagging item to space from space_tagiem()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;
	}

	public boolean space_isclosed(String id, int is_closed) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> space_index = graphDb.index().forNodes("space");

			Node space = space_index.get("id", id).getSingle();
			
			if(space == null)
				throw new RuntimeException("Space not found for the given space id:"+id);
			
			space.setProperty("closed",is_closed);
			
			for(Relationship rel : space.getRelationships(DynamicRelationshipType.withName("Article_Tagged_To_Space"),DynamicRelationshipType.withName("Event_Tagged_To_Space"),DynamicRelationshipType.withName("Petition_Tagged_To_Space"),DynamicRelationshipType.withName("Debate_Tagged_To_Space"),DynamicRelationshipType.withName("Townhall_Tagged_To_Space")))
				rel.getOtherNode(space).setProperty("space", is_closed);
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ space_isclosed('"+id+"',"+is_closed+")");
			System.out.println("Something went wrong, while changinf closed property of the space from space_siclosed()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;
	}
	
	  /*get spaces:
	  @author: Kalyan Kumar Komati
	  @param: user_name , unique user name of the user, user_name can be empty if and only if request/relation type is "so" - open spaces
	  @param: relation_type, type of spaces that user wants to retrieve
	  				f -> user following spaces
	  				c -> spaces created by user
	  				s -> all closed and open spaces, irrespective of user relation with space
	  				sc -> all closed spaces, irrespective of user relation with space
	  				so -> all open spaces, irrespective of user relation with space
	  @param: count, number of space items to be returned
	  @param: prev_cnt, number of space items already read
	  */
	public String get_spaces(String user_name, String relation_type, int count,
			int prev_cnt) throws TException {
		boolean ret = false;
		JSONArray resJSON = new JSONArray();
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> space_index = graphDb.index().forNodes("space");
			Index<Node> user_index = graphDb.index().forNodes("user");

			Node userNode = user_index.get("id", user_name).getSingle();
			IndexHits<Node> allSpaces = space_index.query("id","*");
			
			ArrayList<Node> totSpaces = new ArrayList<Node>();

			if(userNode != null && relation_type.equalsIgnoreCase("f"))
				for(Relationship rel: userNode.getRelationships(DynamicRelationshipType.withName("Space_Followed_By"),Direction.INCOMING))
					totSpaces.add(rel.getOtherNode(userNode));		
			else if(userNode != null && relation_type.equalsIgnoreCase("c"))
				for(Relationship rel: userNode.getRelationships(DynamicRelationshipType.withName("Space_Created_By"),Direction.INCOMING))
					totSpaces.add(rel.getOtherNode(userNode));		
			else if(userNode != null && relation_type.equalsIgnoreCase("s"))
				while(allSpaces.hasNext()) totSpaces.add(allSpaces.next());		
			else if(userNode != null && relation_type.equalsIgnoreCase("sc")){
				while(allSpaces.hasNext()){
					Node space = allSpaces.next();
					if(Integer.parseInt(space.getProperty("closed").toString())==1)
							totSpaces.add(space);
				}
			}
			else if(relation_type.equalsIgnoreCase("so")){
				while(allSpaces.hasNext()){
					Node space = allSpaces.next();
					if(Integer.parseInt(space.getProperty("closed").toString())==0)
							totSpaces.add(space);
				}
			}
			
			Collections.sort(totSpaces, WeightComparatorForSpaces);
			
			int counter = 0;
			
			for(Node space: totSpaces)
			{
				if(counter < prev_cnt){counter++; continue;}
				else if(counter >= count+prev_cnt) break;
				resJSON.put(getSpaceJSONForTile(space, false, userNode));
			}
			
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_spaces('"+user_name+"','"+relation_type+"',"+count+","+prev_cnt+")");
			System.out.println("Something went wrong, while getting space details from get_spaces()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		if(ret)
			return resJSON.toString();
		else return "";
	}

	public boolean user_unfollow(String user_name1, String user_name2)
			throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> user_weight_index = graphDb.index().forNodes("user_weight");
			
			int curTime = (int)(System.currentTimeMillis()/1000) - (86400*2);
			Node user1 = user_index.get("id", user_name1).getSingle();
			Node user2 = user_index.get("id", user_name2).getSingle();

			if(user1 == null)
				throw new RuntimeException("User not found for the given user name:"+user_name1);
			if(user2 == null)
				throw new RuntimeException("User not found for the given user name:"+user_name2);
			
			boolean isFollowing = false;
			
			Relationship followRel = null;
			
			for(Relationship rel: user1.getRelationships(DynamicRelationshipType.withName("Follows"),Direction.OUTGOING))
				if(rel.getEndNode().equals(user2))
				{
					followRel = rel;
					isFollowing = true;
					break;
				}
			
			user1.setProperty("last_seen", curTime);
			
			if(isFollowing && followRel != null){
			int u2_wt = Integer.parseInt(user2.getProperty("weight").toString());
			int del_wt = Integer.parseInt(followRel.getProperty("in_weight").toString());
			
			u2_wt = u2_wt - del_wt;
			user2.setProperty("weight",u2_wt);			
			
			followRel.delete();
			
			user_weight_index.remove(user2);
			user_weight_index.add(user2, "weight", new ValueContext(u2_wt).indexNumeric());
			
			calc_user_tiles(user_name1);
				
			tx.success();
			ret = true;
			}
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ user_unfollow('"+user_name1+"','"+user_name2+"')");
			System.out.println("Something went wrong, while unfollwing user from user_unfollow()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;
	}

	public boolean voteup_markfav_readlater(String user_name, String item_type,
			String item_id, String action, int time) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
	/*		Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> article_weight_index = graphDb.index().forNodes("article_weight");
			Index<Node> event_weight_index = graphDb.index().forNodes("event_weight");
			Index<Node> user_weight_index = graphDb.index().forNodes("user_weight");
			Index<Node> quickpost_weight_index = graphDb.index().forNodes("quickpost_weight");
			Index<Node> hash_weight_index = graphDb.index().forNodes("hash_weight");
			
			Index<Node> index = graphDb.index().forNodes(indexFullName.get(item_type));
			
			int curTime = (int)(System.currentTimeMillis()/1000) - (86400*2);

			Node item = index.get("id", item_id).getSingle();
			Node user = user_index.get("id", user_name).getSingle();

			if(item == null)
				throw new RuntimeException("Item not found for the given item type:"+item_type+" item id:"+item_id);
			if(user == null)
				throw new RuntimeException("User not found for the given user name:"+user_name);

			int item_wwight = Integer.parseInt(item.getProperty("weight").toString());
			int user_wwight = Integer.parseInt(user.getProperty("weight").toString());
			
			user.setProperty("last_seen", curTime);
			
			boolean isAlreadyDone = false;
			
			Relationship actionRel = null;
			
			String relationName = "";
			
			
				*/
			//TODO something
			tx.success();
			ret = true; 
			
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ voteup_markfav_readlater('"+user_name+"','"+item_type+"','"+item_id+"','"+action+"',"+time+")");
			System.out.println("Something went wrong, while performing action from voteup_markfav_readlater()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;
	}

	public boolean votedown(String user_name, String item_type, String item_id,
			int time) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> item_index = graphDb.index().forNodes(indexFullName.get(item_type));
			Index<Node> user_index = graphDb.index().forNodes("user");
	//		Index<Node> user_weight_index = graphDb.index().forNodes("user_weight");
			
			Node item_node = item_index.get("id", item_id).getSingle();
			Node user_node = user_index.get("id", user_name).getSingle();
			
			if(item_node == null)
				throw new RuntimeException("Item not found for the given item type:"+item_type+" item id:"+item_id);
			if(user_node == null)
				throw new RuntimeException("User not found for the given user name:"+user_name);

			int curTime = (int)(System.currentTimeMillis()/1000) - (86400*2);
			user_node.setProperty("last_seen", curTime);
			
			//TODO something
			
			tx.success();
			ret = true; 
			
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ votedown('"+user_name+"','"+item_type+"','"+item_id+"',"+time+")");
			System.out.println("Something went wrong, while performing action from votedown()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;

	}

	public boolean user_remove_hashfav(String user_name, String tag_name)
			throws TException {
/*		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> hash_index = graphDb.index().forNodes("sub_category");
			Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> user_tiles_index = graphDb.index().forNodes("user_tiles");
			Index<Node> article_content_index = graphDb.index().forNodes("article_content");
			
			Node user_node = user_index.get("id", user_name).getSingle();
			
			if(user_node == null)
				throw new RuntimeException("User not found for the given user name:"+user_name);

			int curTime = (int)(System.currentTimeMillis()/1000) - (86400*2);
			user_node.setProperty("last_seen", curTime);
			
			for(String hash : tag_name.split(","))
			{
				Node hashNode = hash_index.get("name", hash.toLowerCase()).getSingle();
				for(Relationship rel: user_node.getRelationships(DynamicRelationshipType.withName("Favourite_Hash")))
				{
					if(rel.getOtherNode(user_node).equals(hashNode))
					{
						rel.delete();
					}
				}
			}

							//Update user personalised tiles
			
			tx.success();
			ret = true; 
			
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ user_remove_hashfav('"+user_name+"','"+tag_name+"')");
			System.out.println("Something went wrong, while performing action from user_remove_hashfav()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret; */
		return false;
	}

	public boolean user_add_hashfav(String user_name, String tag_name)
			throws TException {
//		System.out.println("Sorry, topics and categories were removed. Error at user_add_favtopic");
		return false;
	}

	public boolean user_add_favtopic(String user_name, String topics)
			throws TException {
		System.out.println("Sorry, topics and categories were removed. Error at user_add_favtopic");
		return false;
	}

	public boolean user_view(String user_name, String item_type,
			String item_id, int time) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> item_index = graphDb.index().forNodes(indexFullName.get(item_type));
			Node item_node = item_index.get("id", item_id).getSingle();
			int curTime = (int)(System.currentTimeMillis()/1000) - (86400*2);
			if(item_node == null)
				throw new RuntimeException("Item not found for the given item type:"+item_type+" item id:"+item_id);
			int views = Integer.parseInt(item_node.getProperty("views").toString());
			if(user_name.equals(""))
			{
				
				String latest_views = item_node.getProperty("latest_views").toString();
				if(latest_views.equals(""))
					item_node.setProperty("latest_views",String.valueOf(curTime));
			    else
			    	item_node.setProperty("latest_views",latest_views + "," + String.valueOf(curTime));
				ret = true;
			}
			else
			{
				Index<Node> user_index = graphDb.index().forNodes("user");
				Node user_node = user_index.get("id", user_name).getSingle();
				Node auth_node = null;
				if("article".equals(indexFullName.get(item_type)))
					auth_node = item_node.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getEndNode();
				else if("event".equals(indexFullName.get(item_type)))
					auth_node = item_node.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getEndNode();
				else if("petition".equals(indexFullName.get(item_type)))
					auth_node = item_node.getSingleRelationship(DynamicRelationshipType.withName("Petition_Written_By"),Direction.OUTGOING).getEndNode();
				
				if(user_node != null && !user_node.equals(auth_node))
				{
					user_node.setProperty("last_seen", curTime);
					item_node.setProperty("views", views+1);
					boolean isAlreadyViewed = false;
					for(Relationship rel: item_node.getRelationships(DynamicRelationshipType.withName("Viewed_By"),Direction.OUTGOING))
						if(rel.getOtherNode(item_node).equals(auth_node))
						{
							rel.setProperty("count", Integer.parseInt(rel.getProperty("count").toString())+1);
							rel.setProperty("time", curTime);
							isAlreadyViewed = true;
							break;
						}
					if(isAlreadyViewed == false)
					{
						Relationship rel = user_node.createRelationshipTo(item_node, DynamicRelationshipType.withName("Viewed_By"));
						rel.setProperty("count",1);
						rel.setProperty("time", curTime);
					}
					ret = true;
				}
			}
			tx.success();
		
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ user_view('"+user_name+"','"+item_type+"','"+item_id+"',"+time+")");
			System.out.println("Something went wrong, while incrementing view count from user_view()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		return ret;
	}

	public String get_all_items(String item_type, String user_name, int count,
			int prev_cnt) throws TException {
		boolean ret = false;
		JSONArray resJSON = new JSONArray();
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> tiles_index = graphDb.index().forNodes("tiles");
			Index<Node> item_index = graphDb.index().forNodes(indexFullName.get(item_type));
			
			Node user_node = user_index.get("id",user_name).getSingle();

			String tiles_ids = tiles_index.get("id", indexFullName.get(item_type)).getSingle().getProperty("value").toString();
			
			ArrayList<Node> total_items = new ArrayList<Node>();

			for(String item_id: tiles_ids.split(","))
				if(item_index.get("id", item_id).getSingle()!=null)
					total_items.add(item_index.get("id", item_id).getSingle());
			
			Collections.sort(total_items, TimeCreatedComparatorForNodes);
			
			total_items.trimToSize();
			int totalLen = total_items.size();
			int fromIndex = prev_cnt;
			int toIndex = totalLen < count + prev_cnt ? totalLen : count+prev_cnt;
			if(fromIndex > toIndex)
				toIndex = fromIndex;
			
			List<Node> actualRes = total_items.subList(fromIndex, toIndex);
			
			for(Node item: actualRes)
			{
				if(item.getProperty("__CLASS__").toString().equals("Saddahaq.article"))
					resJSON.put(getArticleJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.event"))
					resJSON.put(getEventJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.petition"))
					resJSON.put(getPetitionJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.townhall"))
					resJSON.put(getTownhallJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.debate"))
					resJSON.put(getDebateJSONForTile(item, false, user_node));
			}
		    
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_all_items('"+item_type+"','"+user_name+"',"+count+","+prev_cnt+")");
			System.out.println("Something went wrong, while reading items from get_all_items()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		if(ret == false)
			return "";
		else
			return resJSON.toString();
		
	}

	public String get_user_data(String user_name) throws TException {
		boolean ret = false;
		JSONObject resJSON = new JSONObject();
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> article_index = graphDb.index().forNodes("article");
			Index<Node> user_tiles_index = graphDb.index().forNodes("user_tiles");
		    
			Node user_node = user_index.get("id",user_name).getSingle();
			if(user_node == null)
				throw new RuntimeException("User not found for the given user name:"+user_name);

			boolean google = false, fb = false, twitter = false;
			
			if(user_node.hasProperty("friends"))
			{
				JSONArray friends = new JSONArray(user_node.getProperty("friends").toString());
				if(!"".equals(friends.getString(0)))
					fb = true;
				if(!"".equals(friends.getString(1)))
					twitter = true;
				if(!"".equals(friends.getString(2)))
					google = true;
				
			}

			StringBuffer follows = new StringBuffer();
			for(Relationship follow : user_node.getRelationships(DynamicRelationshipType.withName("Follows"),Direction.OUTGOING))
				follows.append(","+follow.getOtherNode(user_node).getProperty("user_name").toString());
			if(follows.length() > 0)
				follows.replace(0, 1, "");
			
			StringBuffer following = new StringBuffer();
			for(Relationship follow : user_node.getRelationships(DynamicRelationshipType.withName("Follows"),Direction.INCOMING))
				following.append(","+follow.getOtherNode(user_node).getProperty("user_name").toString());
			if(following.length() > 0)
				following.replace(0, 1, "");

			JSONArray articles = new JSONArray();
			for(Relationship artWritten : user_node.getRelationships(DynamicRelationshipType.withName("Article_Written_By")))
				articles.put(artWritten.getOtherNode(user_node).getProperty("article_title_id").toString());

			JSONArray events = new JSONArray();
			for(Relationship evtWritten : user_node.getRelationships(DynamicRelationshipType.withName("Event_Created_By")))
				events.put(evtWritten.getOtherNode(user_node).getProperty("event_title_id").toString());

			JSONArray petitions = new JSONArray();
			for(Relationship petWritten : user_node.getRelationships(DynamicRelationshipType.withName("Petition_Written_By")))
				petitions.put(petWritten.getOtherNode(user_node).getProperty("p_title_id").toString());

			StringBuffer hashtags = new StringBuffer();
			for(Relationship hashCreated : user_node.getRelationships(DynamicRelationshipType.withName("Hashtag_Created_By")))
				hashtags.append(","+hashCreated.getOtherNode(user_node).getProperty("name").toString());
			if(hashtags.length() > 0)
				hashtags.replace(0, 1, "");

			JSONArray hash_tags = new JSONArray();
			JSONArray persoTiles = new JSONArray();
			JSONArray localTiles = new JSONArray();
			
			for(Relationship hashTagRel: user_node.getRelationships(DynamicRelationshipType.withName("article_voteup"),DynamicRelationshipType.withName("article_voteup"),DynamicRelationshipType.withName("article_voteup")))
			{
				if(hash_tags.length() >= 25) break;
				hash_tags.put(hashTagRel.getOtherNode(user_node).getProperty("name").toString());
			}
			
			for(String loc_art: user_node.getSingleRelationship(DynamicRelationshipType.withName("Belongs_To_Location"),Direction.OUTGOING).getOtherNode(user_node).getProperty("tiles").toString().split(","))
			{
				if(article_index.get("id",loc_art).getSingle() != null)
					localTiles.put(article_index.get("id",loc_art).getSingle().getProperty("article_title_id"));
			}
			Node tile_node = user_tiles_index.get("id",user_name).getSingle();
			for(String pres_art: tile_node.getProperty("news_Personalized").toString().split(","))
			{
				if(article_index.get("id",pres_art).getSingle() != null)
					persoTiles.put(article_index.get("id",pres_art).getSingle().getProperty("article_title_id").toString());
			}
			resJSON.put("ust",user_node.getProperty("acc_type").toString());
			resJSON.put("name",user_node.getProperty("user_name").toString());
			resJSON.put("feed",user_node.getProperty("feed_subscription").toString());
			resJSON.put("topics","");
			resJSON.put("wt",user_node.getProperty("weight"));
			resJSON.put("follows",follows.toString());
			resJSON.put("followers",following.toString());
			resJSON.put("arts",articles);
			resJSON.put("events",events);
			resJSON.put("petitions",petitions);
			resJSON.put("hash_tags",hash_tags);
			resJSON.put("location",user_node.getProperty("location").toString());
			resJSON.put("p_arts",persoTiles);
			resJSON.put("l_arts",localTiles);
			resJSON.put("hashtags",hashtags.toString());
			resJSON.put("google",google);
			resJSON.put("fb",fb);
			resJSON.put("twitter",twitter);
			
			
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_user_data('"+user_name+"')");
			System.out.println("Something went wrong, while reading user info from get_user_data()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		if(ret == false)
			return "";
		else
			return resJSON.toString();
	}

	public String get_item_data(String item_type, String item_id)
			throws TException {
		boolean ret = false;
		JSONObject resJSON = new JSONObject();
		try(Transaction tx = graphDb.beginTx())
		{	
			Index<Node> item_index = graphDb.index().forNodes(indexFullName.get(item_id));
			Index<Node> tiles_index = graphDb.index().forNodes("tiles");
			Node item = item_index.get("id",item_id).getSingle();
			Node newsNode = tiles_index.get("id", "all").getSingle();
			int rank = 0;
			for(String eachId: newsNode.getProperty("value").toString().split(","))
			{
				rank++;
				if(eachId.equalsIgnoreCase(item_id))
					break;
			}
			
			if(item == null)
				throw new RuntimeException("Item not found for the given item type:"+item_type+" item id:"+item_id);
			
			int curTime = (int)(System.currentTimeMillis()/1000);
			int t = curTime - (86400);
			
			if(item_type.toLowerCase().equals("a"))
		    {
				int age = ((curTime - Integer.parseInt(item.getProperty("time_created").toString()))/3600)+1;
				int tot_weight = 0;
				for(Relationship eachAction : item.getRelationships(DynamicRelationshipType.withName("article_voteup"),DynamicRelationshipType.withName("article_markfav"),DynamicRelationshipType.withName("Comment_To_Article")))
				{
					if(Integer.parseInt(eachAction.getProperty("time").toString()) > t)
						tot_weight = tot_weight + Integer.parseInt(eachAction.getProperty("in_weight").toString());
				}
				float rel_wt = ( (Float.parseFloat(item.getProperty("views").toString()) + 1)+(tot_weight *10))/ (age*90);

				StringBuffer locations = new StringBuffer();
				for(Relationship each: item.getRelationships(DynamicRelationshipType.withName("Belongs_To_Location_Article"),Direction.OUTGOING))
				{
					locations.append(","+each.getOtherNode(item).getProperty("location_id").toString());
				}
				if(locations.length() > 1)
					locations.replace(0, 1, "");
				
				StringBuffer hashtags = new StringBuffer();
				for(Relationship each: item.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Article")))
				{
					hashtags.append(","+each.getOtherNode(item).getProperty("name").toString());
				}
				if(hashtags.length() > 1)
					hashtags.replace(0, 1, "");
				
				int a_vu = 0;
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("article_voteup")))
				{	a_vu++; rel.getEndNode();}
				int a_mf = 0;
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("article_markfav")))
				{	a_mf++; rel.getEndNode();}
				int a_noc = 0;
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Comment_To_Article")))
				{	a_noc++; rel.getEndNode();}
				
				resJSON.put("lang", item.getProperty("lang").toString());
				resJSON.put("skip", item.getProperty("skip_from_special_tiles"));
				resJSON.put("a_feu", item.getProperty("approved"));
				resJSON.put("a_title", item.getProperty("article_title").toString());
				resJSON.put("a_content", item.getProperty("article_content").toString());
				resJSON.put("a_weight", item.getProperty("weight"));
				//resJSON.put("a_cat", "No more category");
				resJSON.put("a_ht", hashtags.toString());
				resJSON.put("a_author", item.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getOtherNode(item).getProperty("user_name").toString());
				resJSON.put("a_rank", rank);
				resJSON.put("a_relweight", rel_wt);
				resJSON.put("a_vu", a_vu);
				resJSON.put("a_mf", a_mf);
				resJSON.put("a_noc", a_noc);
				resJSON.put("a_loc", locations.toString());
				resJSON.put("a_age", age);
				//resJSON.put("a_top", "no more topic");
				ret = true;
				
		    }
			else if(item_type.toLowerCase().equals("s"))
		    {

				StringBuffer admins = new StringBuffer();
				for(Relationship each: item.getRelationships(DynamicRelationshipType.withName("Admin_Of_Space")))
				{
					admins.append(","+each.getOtherNode(item).getProperty("user_name").toString());
				}
				if(admins.length() > 1)
					admins.replace(0, 1, "");

				StringBuffer followers = new StringBuffer();
				for(Relationship each: item.getRelationships(DynamicRelationshipType.withName("Space_Followed_By")))
				{
					followers.append(","+each.getOtherNode(item).getProperty("user_name").toString());
				}
				if(followers.length() > 1)
					followers.replace(0, 1, "");

				StringBuffer articles = new StringBuffer();
				for(Relationship each: item.getRelationships(DynamicRelationshipType.withName("Article_Tagged_To_Space")))
				{
					articles.append(","+each.getOtherNode(item).getProperty("article_title_id").toString()+":"+each.getOtherNode(item).getProperty("space"));
				}
				if(articles.length() > 1)
					articles.replace(0, 1, "");


				StringBuffer events = new StringBuffer();
				for(Relationship each: item.getRelationships(DynamicRelationshipType.withName("Event_Tagged_To_Space")))
				{
					events.append(","+each.getOtherNode(item).getProperty("event_title_id").toString()+":"+each.getOtherNode(item).getProperty("space"));
				}
				if(events.length() > 1)
					events.replace(0, 1, "");

				StringBuffer petitions = new StringBuffer();
				for(Relationship each: item.getRelationships(DynamicRelationshipType.withName("Petition_Tagged_To_Space")))
				{
					petitions.append(","+each.getOtherNode(item).getProperty("p_title_id").toString()+":"+each.getOtherNode(item).getProperty("space"));
				}
				if(petitions.length() > 1)
					petitions.replace(0, 1, "");

				StringBuffer townhalls = new StringBuffer();
				for(Relationship each: item.getRelationships(DynamicRelationshipType.withName("Townhall_Tagged_To_Space")))
				{
					townhalls.append(","+each.getOtherNode(item).getProperty("t_title_id").toString()+":"+each.getOtherNode(item).getProperty("space"));
				}
				if(townhalls.length() > 1)
					townhalls.replace(0, 1, "");

				StringBuffer debates = new StringBuffer();
				for(Relationship each: item.getRelationships(DynamicRelationshipType.withName("Debate_Tagged_To_Space")))
				{
					debates.append(","+each.getOtherNode(item).getProperty("d_title_id").toString()+":"+each.getOtherNode(item).getProperty("space"));
				}
				if(debates.length() > 1)
					debates.replace(0, 1, "");
				
				resJSON.put("s_isclosed",item.getProperty("closed"));
				resJSON.put("s_title",item.getProperty("space_title").toString());
				resJSON.put("s_tagline",item.getProperty("space_tagline").toString());
				resJSON.put("s_author",item.getSingleRelationship(DynamicRelationshipType.withName("Space_Created_By"),Direction.OUTGOING).getOtherNode(item).getProperty("user_name"));
				resJSON.put("s_admins",admins.toString());
				resJSON.put("s_followers",followers.toString());
				resJSON.put("s_articles",articles.toString());
				resJSON.put("s_events",events.toString());
				resJSON.put("s_petitions",petitions.toString());
				resJSON.put("s_townhalls",townhalls.toString());
				resJSON.put("s_debates",debates.toString());
				
				ret = true;
		    }
			else if(item_type.toLowerCase().equals("e"))
		    {
				//there is no code written by Yash for events
				ret = false;
		    }
			else if(item_type.toLowerCase().equals("p"))
		    {
				HashSet<String> t1 = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Signed_Petition")))
					t1.add(rel.getOtherNode(item).getProperty("user_name").toString());
				StringBuffer t1Str = new StringBuffer();
				for(String uname: t1)
					t1Str.append(","+uname);
				if(t1Str.length() > 0)
					t1Str.replace(0, 1, "");
				
				StringBuffer hashtags = new StringBuffer();
				for(Relationship each: item.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Petition")))
				{
					hashtags.append(","+each.getOtherNode(item).getProperty("name").toString());
				}
				if(hashtags.length() > 1)
					hashtags.replace(0, 1, "");
				
				resJSON.put("lang",item.getProperty("lang").toString());
				resJSON.put("skip",item.getProperty("skip_from_special_tiles").toString());
				resJSON.put("p_feu",item.getProperty("approved"));
				resJSON.put("p_title",item.getProperty("p_title").toString());
				resJSON.put("p_content",item.getProperty("p_content").toString());
				resJSON.put("p_author",item.getSingleRelationship(DynamicRelationshipType.withName("Petition_Written_By"),Direction.OUTGOING).getOtherNode(item).getProperty("user_name").toString());
				//resJSON.put("p_cat","No more categories");
				resJSON.put("p_ht",hashtags.toString());
				resJSON.put("p_signed",t1Str.toString());
				resJSON.put("p_count",item.getProperty("p_count"));
				resJSON.put("p_target",item.getProperty("p_target"));
				
				ret = true;
		    }
			else if(item_type.toLowerCase().equals("d"))
		    {
				HashSet<String> f = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("For")))
				{	if(Integer.parseInt(rel.getProperty("d_status").toString()) == 1) f.add(rel.getOtherNode(item).getProperty("user_name").toString()); }
				StringBuffer fStr = new StringBuffer();
				for(String uname: f)
					fStr.append(","+uname);
				if(fStr.length() > 0)
					fStr.replace(0, 1, "");

				HashSet<String> a = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Against")))
				{	if(Integer.parseInt(rel.getProperty("d_status").toString()) == 1) a.add(rel.getOtherNode(item).getProperty("user_name").toString()); }
				StringBuffer aStr = new StringBuffer();
				for(String uname: a)
					aStr.append(","+uname);
				if(aStr.length() > 0)
					aStr.replace(0, 1, "");

				HashSet<String> t1 = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Asked_Debate_Question")))
				{	t1.add(rel.getOtherNode(item).getProperty("user_name").toString()); }
				StringBuffer t1Str = new StringBuffer();
				for(String uname: t1)
					t1Str.append(","+uname);
				if(t1Str.length() > 0)
					t1Str.replace(0, 1, "");

				HashSet<String> t2 = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Participated_In_Debate")))
				{	t2.add(rel.getOtherNode(item).getProperty("user_name").toString()); }
				StringBuffer t2Str = new StringBuffer();
				for(String uname: t2)
					t2Str.append(","+uname);
				if(t2Str.length() > 0)
					t2Str.replace(0, 1, "");

				HashSet<String> t3 = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Started_Debate_Argument")))
				{	t3.add(rel.getOtherNode(item).getProperty("user_name").toString()); }
				StringBuffer t3Str = new StringBuffer();
				for(String uname: t3)
					t3Str.append(","+uname);
				if(t3Str.length() > 0)
					t3Str.replace(0, 1, "");

				HashSet<String> t4 = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Commented_On_Debate")))
				{	t4.add(rel.getOtherNode(item).getProperty("user_name").toString()); }
				StringBuffer t4Str = new StringBuffer();
				for(String uname: t4)
					t4Str.append(","+uname);
				if(t4Str.length() > 0)
					t4Str.replace(0, 1, "");
				
				resJSON.put("lang",item.getProperty("lang").toString());
				resJSON.put("skip",item.getProperty("skip_from_special_tiles"));
				resJSON.put("d_title",item.getProperty("d_title").toString());
				resJSON.put("d_content",item.getProperty("d_content").toString());
				resJSON.put("d_author",item.getSingleRelationship(DynamicRelationshipType.withName("Debate_Written_By"),Direction.OUTGOING).getOtherNode(item).getProperty("user_name").toString());
				resJSON.put("d_for",f.toString());
				resJSON.put("d_against",a.toString());
				resJSON.put("d_asked_qtn",t1.toString());
				resJSON.put("d_participate",t2.toString());
				resJSON.put("d_started_arg",t3.toString());
				resJSON.put("d_coms",t4.toString());
				
				ret = true;
				
		    }
			else if(item_type.toLowerCase().equals("t"))
		    {
				HashSet<String> t1 = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Asked_Question")))
					t1.add(rel.getOtherNode(item).getProperty("user_name").toString());
				StringBuffer t1Str = new StringBuffer();
				for(String uname: t1)
					t1Str.append(","+uname);
				if(t1Str.length() > 0)
					t1Str.replace(0, 1, "");

				HashSet<String> t2 = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Participated_In_Townhall")))
					t2.add(rel.getOtherNode(item).getProperty("user_name").toString());
				StringBuffer t2Str = new StringBuffer();
				for(String uname: t2)
					t2Str.append(","+uname);
				if(t2Str.length() > 0)
					t2Str.replace(0, 1, "");

				HashSet<String> t3 = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Voted_Townhall_Question")))
					t3.add(rel.getOtherNode(item).getProperty("user_name").toString());
				StringBuffer t3Str = new StringBuffer();
				for(String uname: t3)
					t3Str.append(","+uname);
				if(t3Str.length() > 0)
					t3Str.replace(0, 1, "");

				HashSet<String> t4 = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Voted_Townhall_Answer")))
					t4.add(rel.getOtherNode(item).getProperty("user_name").toString());
				StringBuffer t4Str = new StringBuffer();
				for(String uname: t4)
					t4Str.append(","+uname);
				if(t4Str.length() > 0)
					t4Str.replace(0, 1, "");

				HashSet<String> t5 = new HashSet<String>();
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Commented_On_Townhall")))
					t5.add(rel.getOtherNode(item).getProperty("user_name").toString());
				StringBuffer t5Str = new StringBuffer();
				for(String uname: t5)
					t5Str.append(","+uname);
				if(t5Str.length() > 0)
					t5Str.replace(0, 1, "");
				
				String t_celeb = "";
				for(Relationship rel: item.getRelationships(DynamicRelationshipType.withName("Townhall_Of")))
				{
					t_celeb = rel.getOtherNode(item).getProperty("user_name").toString();
					break;
				}
				
				resJSON.put("lang",item.getProperty("lang").toString());
				resJSON.put("skip",item.getProperty("skip_from_special_tiles"));
				resJSON.put("t_title",item.getProperty("t_title").toString());
				resJSON.put("t_content",item.getProperty("t_content").toString());
				resJSON.put("t_author",item.getSingleRelationship(DynamicRelationshipType.withName("Townhall_Written_By"),Direction.OUTGOING).getOtherNode(item).getProperty("user_name").toString());
				resJSON.put("t_celeb",t_celeb);
				resJSON.put("t_asked_qtn",t1.toString());
				resJSON.put("t_participate",t2.toString());
				resJSON.put("t_voted_qtn",t3.toString());
				resJSON.put("t_voted_ans",t4.toString());
				resJSON.put("t_coms",t5.toString());
				
				ret = true;
		    }
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_item_data('"+item_type+"','"+item_id+"')");
			System.out.println("Something went wrong, while reading user info from get_user_data()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		if(ret == false)
			return "";
		else
			return resJSON.toString();
	}

	public String get_monthly_items() throws TException {
		// TODO returns all the float values, dont know why
		return "";
	}

	public boolean article_pushed(String id) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{	
			Index<Node> item_index = graphDb.index().forNodes("article");
			Node item = item_index.get("id",id).getSingle();
			if(item!= null && item.hasRelationship(DynamicRelationshipType.withName("Pushed_By"),Direction.OUTGOING))
				ret = true;
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ article_pushed('"+id+"')");
			System.out.println("Something went wrong, while checking article is pushed or not from article_pushed()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public String stream(String user_name, String item_type, int count,
			int prev_cnt) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String context(String user_name, String item_type, String item_id,
			int count, int prev_cnt) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String notifications(String user_name, int count, int prev_cnt)
			throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public boolean create_article(String user_name, String a_id,
			String a_title_id, String a_title, String a_content,
			String a_summary, String a_fut_image, String a_cat,
			String a_subcat, String a_hashtags, String a_users,
			int a_time_created, String related_articles, String related_events,
			String mod_name, int stars, int is_edit, int is_closed, String lang)
			throws TException {
		
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{	
			if(a_id == null || a_id.equalsIgnoreCase(""))
				throw new RuntimeException("Invalid article id. Received either null or empty string");
			if(a_title_id == null || a_title_id.equalsIgnoreCase(""))
				throw new RuntimeException("Invalid title id. Received either null or empty string.");
			if(a_title == null )
				throw new RuntimeException("Invalid title. Received null.");
			if(a_content == null )
				throw new RuntimeException("Invalid content. Received null.");
			if(a_summary == null )
				throw new RuntimeException("Invalid summery. Received null.");
			if(a_fut_image == null || a_fut_image.equalsIgnoreCase(""))
				throw new RuntimeException("Invalid image. Received either null or empty string.");
			if(a_subcat == null || a_subcat.equals(""))
				throw new RuntimeException("Invalid a_subcat. Received either null or empty string.");
			if(a_hashtags == null )
				throw new RuntimeException("Invalid a_hashtags. Received null.");
			if(a_users == null)
				throw new RuntimeException("Invalid a_users. Received null.");
			if(related_articles == null)
				throw new RuntimeException("Invalid related articles. Received null.");
			if(related_events == null)
				throw new RuntimeException("Invalid related events. Received null");
			if(mod_name == null)
				throw new RuntimeException("Invalid mod_name. Received null");
			if("".equals(a_content) && a_title != null)
				a_content = a_title;
			
			aquireWriteLock(tx, "article");
			
			Index<Node> user_index = graphDb.index().forNodes("user");
			Node user = user_index.get("id",user_name).getSingle();
			
			if(user == null)
				throw new RuntimeException("User not found for the given user name:"+user_name);
			
			Index<Node> article_index = graphDb.index().forNodes("article");
			Node article = article_index.get("id", a_id).getSingle();
			
			if(article == null && is_edit == 1)
				throw new RuntimeException("Article not found for the given article id : "+a_id + ", unable to edit article.");
			if(article != null && is_edit == 0)
				throw new RuntimeException("An article already exists with given id : "+a_id + ", unable to create article.");

			Index<Node> event_index = graphDb.index().forNodes("event");
			Node event = event_index.get("id", a_id).getSingle();
			Index<Node> petition_index = graphDb.index().forNodes("petition");
			Node petition = petition_index.get("id", a_id).getSingle();
			Index<Node> townhall_index = graphDb.index().forNodes("townhall");
			Node townhall = townhall_index.get("id", a_id).getSingle();
			Index<Node> debate_index = graphDb.index().forNodes("debate");
			Node debate = debate_index.get("id", a_id).getSingle();
			Index<Node> space_index = graphDb.index().forNodes("space");
			Node space = space_index.get("id", a_id).getSingle();

			if(event != null)
				throw new RuntimeException("An event is already exists with given id: "+a_id+", unable to create/edit article");
			if(petition != null)
				throw new RuntimeException("A petition is already exists with given id: "+a_id+", unable to create/edit article");
			if(townhall != null)
				throw new RuntimeException("A townhall is already exists with given id: "+a_id+", unable to create/edit article");
			if(debate != null)
				throw new RuntimeException("A debate is already exists with given id: "+a_id+", unable to create/edit article");
			if(space != null)
				throw new RuntimeException("A space is already exists with given id: "+a_id+", unable to create/edit article");
			
			Index<Node> tiles_index = graphDb.index().forNodes("tiles");
			Index<Node> article_content_index = graphDb.index().forNodes("article_content");
			Index<Node> article_title_index = graphDb.index().forNodes("article_title");
			Index<Node> article_weight_index = graphDb.index().forNodes("article_weight");
			Index<Node> user_weight_index = graphDb.index().forNodes("user_weight");
			Index<Node> hash_weight_index = graphDb.index().forNodes("hash_weight");
			Index<Node> location_index = graphDb.index().forNodes("location");
			Index<Node> hashtag_index = graphDb.index().forNodes("sub_category");
			Index<Node> user_tiles_index = graphDb.index().forNodes("user_tiles");
			
			String index_data_for_cities = bummy(a_title+" "+a_summary+" "+a_content)+" "+a_subcat.replaceAll(",", " ");
			
			int article_wt = 10;
			
			boolean isCreatedOrEdited = false;
			
			if(is_edit == 0)
			{
				//create new article
				int curTime = (int)(System.currentTimeMillis()/1000);
				user.setProperty("last_seen",a_time_created);
				int user_wt = Integer.parseInt(user.getProperty("weight").toString());
				article_wt = Math.round(((float)user_wt)/100);
				int weight = 0;
				
				article = article(a_id,a_title_id,a_title,"",a_summary,a_fut_image,a_time_created,article_wt,0,1,0,"",stars,0,0,is_closed, 0,lang);
				
				article_index.add(article, "id", a_id);
				
				article_content_index.add(article, "time", new ValueContext( a_time_created ).indexNumeric());
				
				article_title_index.add(article, "title_id", a_title_id);
				
				Node weights_node = item_trending_weights(a_id,"1",0.0f, 1.0f, 1.0f);
				article.createRelationshipTo(weights_node, DynamicRelationshipType.withName("item_trending_weights"));
				Relationship art_written_by_rel = article.createRelationshipTo(user, DynamicRelationshipType.withName("Article_Written_By"));
				art_written_by_rel.setProperty("time", a_time_created);
				art_written_by_rel.setProperty("in_weight", 10 + weight);
				art_written_by_rel.setProperty("out_weight", article_wt + weight);
				
				article_weight_index.add(article,"weight",new ValueContext( article_wt + weight ).indexNumeric());
				user.setProperty("weight",user_wt+10+weight);
				user_weight_index.remove(user);
				user_weight_index.add(user,"weight",new ValueContext( user_wt + 10 + weight ).indexNumeric());
				
				if((is_closed == 0) && ((curTime - a_time_created) < (24*60*60)))
				{
					Node tilesNode = tiles_index.get("id", "trending").getSingle();
					if(tilesNode == null){
						System.out.println("new node creating");
						tiles_index.add(tiles("trending",a_id), "id", "trending");
					}
					else
					{
						String tiles = a_id + "," + tilesNode.getProperty("value").toString();
						tilesNode.setProperty("value",tiles);
					}
				}
				
				if(is_closed == 0)
					for(Relationship userFollowRel : user.getRelationships(DynamicRelationshipType.withName("Follows"),Direction.INCOMING))
					{
						Node each = userFollowRel.getOtherNode(user);
						String eachUserName = each.getProperty("u_name").toString();
						
						Node user_tiles_node = user_tiles_index.get("id", eachUserName).getSingle();
						String perso = user_tiles_node.getProperty("news_Personalized").toString();
						if(perso.equals(""))
							user_tiles_node.setProperty("news_Personalized",a_id);
						else
							user_tiles_node.setProperty("news_Personalized", perso + "," + a_id);
					}
				
				isCreatedOrEdited = true;
				
			}
			else{
				//edit existing article
				user.setProperty("last_seen",a_time_created);
				article_wt = Integer.parseInt(article.getProperty("weight").toString());
				article.setProperty("article_title_id",a_title_id);
				article.setProperty("article_title",a_title);
				article.setProperty("article_summary",a_summary);
				article.setProperty("article_featured_img",a_fut_image);
				article.setProperty("stars",stars);
				article.setProperty("lang",lang);
				
				article_content_index.remove(article);
				article_content_index.add(article, "time", new ValueContext( a_time_created ).indexNumeric());
				article_title_index.remove(article);
				article_title_index.add(article, "title_id", a_title_id);
				
				for(Relationship toRemoveRel : article.getRelationships(DynamicRelationshipType.withName("Belongs_To_Location_Article"),
																		DynamicRelationshipType.withName("User_Of_Article"),
																		DynamicRelationshipType.withName("Related_Article_To"),
																		DynamicRelationshipType.withName("Related_Event_To"),
																		DynamicRelationshipType.withName("Tag_Of_Article"),
																		DynamicRelationshipType.withName("Belongs_To_Subcategory_Article")))
					{
						toRemoveRel.delete();
					}
				
				isCreatedOrEdited = true;
				
			}
			
			if(isCreatedOrEdited)
			{
				HashSet<String> allCities = new HashSet<String>();
				for(String city: location_index.get("id","all").getSingle().getProperty("cities").toString().split(","))
					allCities.add(city);
				for(String word: index_data_for_cities.toLowerCase().split(" "))
					if(allCities.contains(word))
						{
							IndexHits<Node> locationNodes = location_index.query("id","*");
							while(locationNodes.hasNext())
							{
								Node location = locationNodes.next();
								if(location.getProperty("cities").toString().contains(word))
								{
									article.createRelationshipTo(location, DynamicRelationshipType.withName("Belongs_To_Location_Article"));
									break;
								}
							}
						}
				
	
				if(!related_articles.equals(""))
					for(String artId : related_articles.split(","))
					{
						Node art = article_index.get("id", artId).getSingle();
						if(art != null)
							art.createRelationshipTo(article, DynamicRelationshipType.withName("Related_Article_To")).setProperty("time", a_time_created);
					}
				
				if(!related_events.equals(""))
					for(String evtId : related_events.split(","))
					{
						Node evt = event_index.get("id", evtId).getSingle();
						if(evt != null)
							evt.createRelationshipTo(article, DynamicRelationshipType.withName("Related_Event_To")).setProperty("time", a_time_created);
					}
				
				HashSet<Node> uniqueHashTags = new HashSet<Node>();
				
				boolean mainHash = true;
				for(String hash : a_subcat.split(","))
				{
					hash = hash.toLowerCase();
					if(hash.equals(""))
						continue;
					Node hashTagNode = hashtag_index.get("name", hash).getSingle();
					if(hashTagNode == null)
					{
						hashTagNode = sub_category(hash, a_time_created, 0);
						hashtag_index.add(hashTagNode, "name",hash);
						hashTagNode.createRelationshipTo(user, DynamicRelationshipType.withName("Hashtag_Created_By")).setProperty("time", a_time_created);
					}
					else hash_weight_index.remove(hashTagNode);
					
					uniqueHashTags.add(hashTagNode);
					
					int h_wt = Integer.parseInt(hashTagNode.getProperty("weight").toString());
					
					Relationship rel = article.createRelationshipTo(hashTagNode, DynamicRelationshipType.withName("Belongs_To_Subcategory_Article"));
					if(mainHash)
					{
						rel.setProperty("main", 1);
						mainHash = false;
					}
					rel.setProperty("time",a_time_created);
					rel.setProperty("in_weight", article_wt);
					rel.setProperty("out_weight", 0);
					
					hash_weight_index.add(hashTagNode, "weight", new ValueContext( h_wt + article_wt ).indexNumeric());
				}
				
				for(String hash : a_hashtags.split(","))
				{
					hash = hash.toLowerCase();
					if(hash.equals(""))
						continue;
					Node hashTagNode = hashtag_index.get("name", hash).getSingle();
					if(hashTagNode == null)
					{
						hashTagNode = sub_category(hash, a_time_created, 0);
						hashtag_index.add(hashTagNode, "name",hash);
						hashTagNode.createRelationshipTo(user, DynamicRelationshipType.withName("Hashtag_Created_By")).setProperty("time", a_time_created);
					}
					else hash_weight_index.remove(hashTagNode);
					
					uniqueHashTags.add(hashTagNode);
					
					int h_wt = Integer.parseInt(hashTagNode.getProperty("weight").toString());
					
					Relationship rel = article.createRelationshipTo(hashTagNode, DynamicRelationshipType.withName("Tag_Of_Article"));
					
					rel.setProperty("time",a_time_created);
					rel.setProperty("in_weight", 0);
					rel.setProperty("out_weight", 10);
					
					hash_weight_index.add(hashTagNode, "weight", new ValueContext( h_wt + 10 ).indexNumeric());
				}
				
				for(String aUser : a_users.split(","))
				{
					Node aUserNode = user_index.get("id",aUser).getSingle();
					if(aUserNode != null)
						aUserNode.createRelationshipTo(article, DynamicRelationshipType.withName("User_Of_Article"));
				}
				
				HashSet<Node> visited = new HashSet<Node>();
				HashSet<Node> unVisited = new HashSet<Node>();
				unVisited.addAll(uniqueHashTags);
				
				boolean hasRel = false;
				
				for(Node eachTag : uniqueHashTags)
				{
					visited.add(eachTag);
					unVisited.remove(eachTag);
					
					for(Node each : unVisited)
					{
						hasRel = false;
						
						for(Relationship tagToTagRel : each.getRelationships(DynamicRelationshipType.withName("Tag_To_Tag")))
						{
							if(tagToTagRel.getOtherNode(each).equals(eachTag))
							{
								hasRel = true;
								int count = Integer.parseInt(tagToTagRel.getProperty("count").toString());
								tagToTagRel.setProperty("count", count+1);
								break;
							}
						}
						
						if(!hasRel)
							eachTag.createRelationshipTo(each, DynamicRelationshipType.withName("Tag_To_Tag")).setProperty("count", 1);
					}
				}
				
	
	//			MaxentTagger tagger = new MaxentTagger(
	//	                "/var/n4j/data/left3words-wsj-0-18.tagger");
	//			String tagged = tagger.tagString(a_content);
				String tagged = a_content;
				ArrayList<String> proper_nouns = new ArrayList<String>();
				proper_nouns.add("2013");
				proper_nouns.add("2014");
				proper_nouns.add("india");
				proper_nouns.add("indian");
				proper_nouns.add("reddy");
				proper_nouns.add("rao");
				proper_nouns.add("singh");
				proper_nouns.add("world");
				proper_nouns.add("pti");
				proper_nouns.add("sunday");
				proper_nouns.add("monday");
				proper_nouns.add("tuesday");
				proper_nouns.add("wednesday");
				proper_nouns.add("thursday");
				proper_nouns.add("friday");
				proper_nouns.add("saturday");
				proper_nouns.add("january");
				proper_nouns.add("february");
				proper_nouns.add("march");
				proper_nouns.add("april");
				proper_nouns.add("may");
				proper_nouns.add("june");
				proper_nouns.add("july");
				proper_nouns.add("august");
				proper_nouns.add("september");
				proper_nouns.add("october");
				proper_nouns.add("november");
				proper_nouns.add("december");
				
				StringBuffer noun_list = new StringBuffer();
				for(String word: tagged.split(" "))
				{
					String[] tokens = word.split("/");
					if(tokens.length == 2 && tokens[1].equals("NNP") && tokens[0].length() >= 3 && !proper_nouns.contains(tokens[0]))
						noun_list.append(" " + tokens[0].toLowerCase());
				}
				for(Node eachTag : uniqueHashTags)
					noun_list.append(" " + eachTag.getProperty("name").toString());
				if(noun_list.length() > 0)
					noun_list.replace(0, 1, "");
				
				article_content_index.add(article,"article_content",(noun_list.toString() + " " + bummy(a_title)));
				
				tx.success();
				ret = true;
			}
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ create_article('"+user_name+"','"+a_id+"','"+a_title_id+"','"+a_title+"','"+a_content
					+"','"+a_summary+"','"+a_fut_image+"','"+a_subcat+"','"+a_hashtags+"','"+a_users+"',"+a_time_created+",'"+related_articles
					+"','"+related_events+"','"+mod_name+"',"+stars+","+is_edit+","+is_closed+",'"+lang+"')");
			System.out.println("Something went wrong, while creating / editing article from create_article()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
		
	}
	
	public String bummy(String word)
	{
		/*
		    
    	val common_words = List("other", "tried", "become", "therefore", "ending", "about", "less", "they’ve",
	        "group", "really", "put", "finds", "whereby", "yours", "into", "z", "ignored", "much", "weren't",
	        "getting", "years", "seemed", "outside", "furthers", "yes", "got", "it", "did", "theres",
	        "while", "newer", "presented", "cant", "ought", "won’t", "her", "indicated", "they'll", "meanwhile",
	        "differ", "up", "nevertheless", "let", "today", "he's", "too", "neither", "hither", "c", "sup", "the",
	        "clear", "which", "mustn't", "its", "tends", "knew", "they", "concerning", "would", "seriously", "ends",
	        "hasn't", "same", "our", "either", "older", "evenly", "inasmuch", "going", "could", "they'd", "let’s",
	        "show", "ours", "que", "only", "t", "how", "lately", "ever", "et", "m", "taken", "itself", "weren’t",
	        "every", "thereafter", "whether", "they’re", "selves", "parting", "seven", "whatever", "know", "behind",
	        "allows", "they’d", "without", "wherever", "though", "myself", "year", "accordingly", "indeed", "y",
	        "wasn’t", "can’t", "first", "third", "been", "co", "sensible", "wasn't", "differently", "under", "soon",
	        "you’ve", "mainly", "making", "useful", "inc", "f", "it’s", "who", "certain", "who's", "relatively",
	        "some", "you", "want", "then", "sees", "p", "perhaps", "places", "followed", "becomes", "asks", "that's",
	        "downed", "who’s", "say", "different", "th", "take", "they've", "however", "considering", "except",
	        "point", "ordering", "downs", "in", "entirely", "believe", "latterly", "must", "whose", "it’ll",
	        "happens", "him", "made", "beings", "wouldn’t", "doesn't", "ie", "namely", "therein", "you'd", "sub",
	        "needed", "next", "inward", "i’m", "aren't", "r", "thanks", "does", "had", "normally", "sure", "anyways",
	        "among", "done", "currently", "asking", "still", "out", "course", "he’s", "it’d", "instead", "whither",
	        "doesn’t", "obviously", "thence", "we’ve", "above", "high", "don’t", "there’s", "ways", "novel", "will",
	        "is", "hello", "we’re", "set", "he'd", "enough", "merely", "rather", "pointed", "works", "viz", "saw", "old",
	        "once", "until", "let's", "were", "kind", "presumably", "but", "wish", "regards", "am", "com", "like", "contain",
	        "hasn’t", "what's", "wonder", "your", "described", "welcome", "couldn’t", "we're", "she'd", "cause", "says",
	        "wanting", "twice", "e", "wherein", "serious", "again", "plus", "afterwards", "big", "use", "l", "noone",
	        "you’d", "upon", "c’mon", "quite", "consequently", "at", "herein", "isn't", "especially", "greetings",
	        "seeming", "here", "something", "across", "aside", "around", "unfortunately", "needing", "theirs", "nobody",
	        "new", "that", "respectively", "where", "are", "causes", "be", "mean", "indicates", "grouping", "turns",
	        "placed", "couldn't", "probably", "near", "tries", "hereby", "shows", "changes", "from", "such", "having",
	        "six", "always", "face", "awfully", "vs", "ones", "yourselves", "area", "needs", "you’re", "none", "latter",
	        "thru", "furthering", "presents", "little", "we'll", "q", "being", "where's", "longer", "inner", "best", "former",
	        "comes", "zero", "good", "have", "hopefully", "yourself", "wells", "whereafter", "w", "further", "points",
	        "although", "available", "wouldn't", "look", "presenting", "i've", "everywhere", "specifying", "when", "over",
	        "fully", "he", "here's", "never", "also", "h", "thorough", "whole", "opened", "think", "men", "not", "seem",
	        "howbeit", "said", "along", "any", "right", "seeing", "if", "ended", "four", "me", "goes", "might", "together",
	        "j", "using", "later", "back", "qv", "furthermore", "everybody", "overall", "last", "see", "according", "showing",
	        "gets", "self", "un", "s", "hereupon", "she", "began", "kept", "we’d", "now", "corresponding", "all", "i'll",
	        "that’s", "sometime", "keep", "appreciate", "whence", "moreover", "should", "opens", "looking", "there's",
	        "they’ll", "after", "grouped", "showed", "full", "through", "tell", "give", "unto", "became", "elsewhere",
	        "what", "thanx", "whereas", "you’ll", "likely", "eg", "backed", "away", "whereupon", "provides", "you're",
	        "gives", "numbers", "please", "able", "used", "as", "ok", "backs", "thank", "rd", "than", "d", "alone",
	        "willing", "has", "hers", "own", "hence", "etc", "thinks", "since", "so", "don't", "anyhow", "mostly",
	        "haven’t", "five", "three", "various", "she's", "liked", "turn", "we'd", "order", "work", "edu", "everything",
	        "i'm", "between", "he'll", "another", "go", "it's", "brief", "this", "just", "despite", "several", "man",
	        "besides", "these", "thats", "hadn’t", "anyone", "downing", "worked", "v", "certainly", "seen", "o", "somebody",
	        "sorry", "two", "i'd", "their", "open", "nd", "felt", "came", "we", "general", "someone", "thoroughly", "was",
	        "make", "anywhere", "immediate", "often", "looks", "both", "orders", "groups", "appear", "far", "very", "generally",
	        "largely", "doing", "there", "cases", "himself", "gone", "apart", "faces", "turned", "maybe", "becoming", "ltd",
	        "c’s", "one", "latest", "necessary", "turning", "exactly", "i’ve", "because", "parts", "few", "via", "consider",
	        "ordered", "things", "parted", "down", "associated", "known", "non", "appropriate", "help", "unless", "early",
	        "i", "long", "well", "oh", "furthered", "toward", "possible", "haven't", "what’s", "during", "contains", "nothing",
	        "off", "hadn't", "specify", "okay", "hi", "re", "containing", "following", "k", "whenever", "clearly", "specified",
	        "anyway", "throughout", "least", "name", "somewhat", "place", "shall", "sometimes", "anybody", "you'll", "need",
	        "or", "nine", "didn’t", "saying", "wants", "i’ll", "here’s", "puts", "reasonably", "regardless", "we've", "lest",
	        "and", "backing", "usually", "towards", "unlikely", "somewhere", "uses", "thing", "gave", "nowhere", "forth", "whom",
	        "get", "follows", "thereupon", "went", "sent", "trying", "can't", "you've", "anything", "ex", "secondly", "no",
	        "find", "can", "beside", "fifth", "b", "ask", "formerly", "hereafter", "allow", "truly", "come", "below",
	        "beforehand", "otherwise", "u", "on", "us", "thus", "ourselves", "his", "gotten", "onto", "why", "when's",
	        "n", "won't", "themselves", "a", "amongst", "before", "try", "by", "nearly", "lets", "g", "number", "per",
	        "indicate", "working", "we’ll", "particularly", "mr", "isn’t", "asked", "side", "mrs", "of", "second", "my",
	        "a’s", "end", "most", "case", "took", "opening", "those", "already", "eight", "seems", "with", "given", "more",
	        "insofar", "value", "i’d", "each", "particular", "how's", "part", "areas", "example", "cannot", "them", "definitely",
	        "somehow", "whoever", "within", "for", "do", "everyone", "almost", "didn't", "sides", "shan't", "t’s", "keeps",
	        "others", "regarding", "x", "aren’t", "else", "actually", "knows", "thereby", "herself", "ain’t", "downwards",
	        "against", "she'll", "to", "nor", "better", "may", "shouldn’t", "hardly", "why's", "even", "yet", "they're",
	        "way", "many", "beyond", "an", "where’s", "shouldn't", "help", "join", "joined")


    		val it = (word.replaceAll("[^A-Za-z0-9]", " ")).split(" ").toList
    		val f = it.filter(x => common_words.contains(x.toLowerCase())==false && !x.equals(""))
    		f.mkString(" ")
		 */
		return  word.replaceAll("[^A-Za-z0-9]", " ").toLowerCase();
	}
	
	public boolean create_cfpost(String cf_user, String cf_id, String cf_title,
			String cf_url, String cf_content, String cf_fut_image,
			String cf_hashtags, int cf_time_created, int cf_end_date,
			int cf_amt_target, int cf_amt_raised, int cf_ppl_count, int is_edit)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean create_poll(String item_type, String item_id, String p_id,
			String p_qtn, int p_status, int p_time_created) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean respond_poll(String user_name, String p_id, int time)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean featured_item(String item_type, String item_id)
			throws TException {
		
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{	
			Index<Node> tiles_index = graphDb.index().forNodes("tiles");
			Index<Node> item_index = graphDb.index().forNodes(indexFullName.get(item_type));
			
			Node item_node = item_index.get("id", item_id).getSingle();
			Node featuredNode = tiles_index.get("id", "featured").getSingle();
			Node latestNode = tiles_index.get("id", "latest").getSingle();
			if(item_node != null)
			{
				if(Integer.parseInt(item_node.getProperty("approved").toString())==0)
				{
					//make featured
					item_node.setProperty("approved",1);
					if(featuredNode != null)
						featuredNode.setProperty("value",item_id + "," + featuredNode.getProperty("value").toString());
					else tiles_index.add(tiles("featured", item_id), "id", "featured");
					if(latestNode != null){
						StringBuffer latestitems = new StringBuffer();
						int count  = 0;
						for(String each : (item_id + "," + latestNode.getProperty("value").toString()).split(","))
						{
							if(count == 30) break;
							count++;
							latestitems.append(","+each);
						}
						if(latestitems.length() > 0)
							latestitems.replace(0, 1, "");
						latestNode.setProperty("value",latestitems.toString());
					}
					else tiles_index.add(tiles("latest", item_id), "id", "latest");
				}
				else
				{
					//remove from featured
					item_node.setProperty("approved",0);
					//unpin the item from pinned items if it is pinned previously
					pin_item(item_type.toUpperCase(), item_id, "f", "");
					//remove id from featured list
					if(featuredNode != null)
					{
						StringBuffer featuredString = new StringBuffer();
						for(String eachFeaturedId : featuredNode.getProperty("value").toString().split(","))
							if(!eachFeaturedId.equalsIgnoreCase(item_id))
								featuredString.append(","+eachFeaturedId);
						if(featuredString.length() > 1)
							featuredString.replace(0,1,"");
						featuredNode.setProperty("value",featuredString.toString());
					}
					if(latestNode != null)
					{
						StringBuffer latestString = new StringBuffer();
						for(String eachLatestId : latestNode.getProperty("value").toString().split(","))
							if(!eachLatestId.equalsIgnoreCase(item_id))
								latestString.append(","+eachLatestId);
						if(latestString.length() > 1)
							latestString.replace(0,1,"");
						latestNode.setProperty("value",latestString.toString());
					}
				}
				
			}
			else throw new RuntimeException("Item not found for the given type : " + item_type + " and item id : " + item_id);
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ featured_item('"+item_type+"','"+item_id+"')");
			System.out.println("Something went wrong, while featuring an item from featured_item()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
		
	}

	public boolean headlines_item(String item_type, String item_id)
			throws TException {

		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{	
			Index<Node> tiles_index = graphDb.index().forNodes("tiles");
			Index<Node> item_index = graphDb.index().forNodes(indexFullName.get(item_type));
			
			Node item_node = item_index.get("id", item_id).getSingle();
			Node headlinesNode = tiles_index.get("id", "headlines").getSingle();
			if(item_node != null)
			{
				if(Integer.parseInt(item_node.getProperty("head").toString())==0)
				{
					//make headline
					item_node.setProperty("head",1);
					if(headlinesNode != null)
						headlinesNode.setProperty("value",item_id + "," + headlinesNode.getProperty("value").toString());
					else tiles_index.add(tiles("headlines", item_id), "id", "headlines");
				}
				else
				{
					//remove from headline
					item_node.setProperty("head",0);
					//remove id from headline list
					if(headlinesNode != null)
					{
						StringBuffer headlinesString = new StringBuffer();
						for(String eachHeadlineId : headlinesNode.getProperty("value").toString().split(","))
							if(!eachHeadlineId.equalsIgnoreCase(item_id))
								headlinesString.append(","+eachHeadlineId);
						if(headlinesString.length() > 1)
							headlinesString.replace(0,1,"");
						headlinesNode.setProperty("value",headlinesString.toString());
					}
				}
				
			}
			else throw new RuntimeException("Item not found for the given type : " + item_type + " and item id : " + item_id);
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ headlines_item('"+item_type+"','"+item_id+"')");
			System.out.println("Something went wrong, while headline an item from headlines_item()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public String get_tiles(String user_name, String cat, int count,
			int prev_cnt, String art_id, String tiles_type) throws TException {
		boolean ret = false;
		JSONArray resJSON = new JSONArray();
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> tiles_index = graphDb.index().forNodes("tiles");
			Index<Node> user_index = graphDb.index().forNodes("user");
			Index<Node> article_index = graphDb.index().forNodes("article");
			Index<Node> event_index = graphDb.index().forNodes("event");
			Index<Node> petition_index = graphDb.index().forNodes("petition");
			Index<Node> townhall_index = graphDb.index().forNodes("townhall");
			Index<Node> debate_index = graphDb.index().forNodes("debate");
			Index<Node> userTiles_index = graphDb.index().forNodes("user_tiles");

			Index<Node> decommisioned_index = graphDb.index().forNodes("decommissioned_tiles");
			Node decommisionedList_node = decommisioned_index.get("id","skip_items").getSingle();
			
			String decommisioned_ids = "";
			if( decommisionedList_node != null)
				decommisioned_ids = decommisionedList_node.getProperty("value").toString();
			
			Node user_node = user_index.get("id",user_name).getSingle();

			String tiles_ids = "";
			String pinned_tiles_ids = "";
			
			ArrayList<Node> total_items = new ArrayList<Node>();
			ArrayList<Node> pinned_items = new ArrayList<Node>();
			
			if("f".equalsIgnoreCase(tiles_type)) //featured
			{
				Node pinned_node = tiles_index.get("name","featured_pinned").getSingle();
				Node news_node = tiles_index.get("id","featured").getSingle();
				if(news_node != null)
				{
					tiles_ids = news_node.getProperty("value").toString();
					if(pinned_node != null && pinned_node.hasProperty("value"))
						pinned_tiles_ids = pinned_node.getProperty("value").toString();
					
					for(String itemId : pinned_tiles_ids.split(","))
					{
						if(pinned_items.size() == 1)
							break;
						if(decommisioned_ids.contains(itemId))
							continue;
						if(article_index.get("id",itemId).getSingle() != null)
							pinned_items.add(article_index.get("id",itemId).getSingle());
						else if(event_index.get("id",itemId).getSingle() != null)
							pinned_items.add(event_index.get("id",itemId).getSingle());
						else if(petition_index.get("id",itemId).getSingle() != null)
							pinned_items.add(petition_index.get("id",itemId).getSingle());
						else if(townhall_index.get("id",itemId).getSingle() != null)
							pinned_items.add(townhall_index.get("id",itemId).getSingle());
						else if(debate_index.get("id",itemId).getSingle() != null)
							pinned_items.add(debate_index.get("id",itemId).getSingle());
					}
					
					for(String itemId : tiles_ids.split(","))
					{
						if(decommisioned_ids.contains(itemId))
							continue;
						if(article_index.get("id",itemId).getSingle() != null)
							total_items.add(article_index.get("id",itemId).getSingle());
						else if(event_index.get("id",itemId).getSingle() != null)
							total_items.add(event_index.get("id",itemId).getSingle());
						else if(petition_index.get("id",itemId).getSingle() != null)
							total_items.add(petition_index.get("id",itemId).getSingle());
						else if(townhall_index.get("id",itemId).getSingle() != null)
							total_items.add(townhall_index.get("id",itemId).getSingle());
						else if(debate_index.get("id",itemId).getSingle() != null)
							total_items.add(debate_index.get("id",itemId).getSingle());
					}
					ret = true;
				}
				else
					throw new RuntimeException("Unable to find featured tiles node :");
			}
			else if("h".equalsIgnoreCase(tiles_type)) //headlines
			{
				Node news_node = tiles_index.get("id","headlines").getSingle();
				if(news_node != null)
				{
					tiles_ids = news_node.getProperty("value").toString();
					
					pinned_items.clear();
					
					for(String itemId : tiles_ids.split(","))
					{
						if(decommisioned_ids.contains(itemId))
							continue;
						
						if(article_index.get("id",itemId).getSingle() != null)
							total_items.add(article_index.get("id",itemId).getSingle());
						else if(event_index.get("id",itemId).getSingle() != null)
							total_items.add(event_index.get("id",itemId).getSingle());
						else if(petition_index.get("id",itemId).getSingle() != null)
							total_items.add(petition_index.get("id",itemId).getSingle());
						else if(townhall_index.get("id",itemId).getSingle() != null)
							total_items.add(townhall_index.get("id",itemId).getSingle());
						else if(debate_index.get("id",itemId).getSingle() != null)
							total_items.add(debate_index.get("id",itemId).getSingle());
					}
					ret = true;
				}
				else
					throw new RuntimeException("Unable to find headlines tile node :");
			}
			else if("up".equalsIgnoreCase(tiles_type)) //user profile page
			{
				Node user_node1 = user_index.get("id",art_id).getSingle();
				
				if(user_node1 != null)
				{
					if(user_node1.hasProperty("pins"))
						pinned_tiles_ids = user_node1.getProperty("pins").toString();
					
					for(String itemId : pinned_tiles_ids.split(","))
					{
						if(pinned_items.size() == 1)
							break;
						if(decommisioned_ids.contains(itemId))
							continue;
						if(article_index.get("id",itemId).getSingle() != null)
							pinned_items.add(article_index.get("id",itemId).getSingle());
						else if(event_index.get("id",itemId).getSingle() != null)
							pinned_items.add(event_index.get("id",itemId).getSingle());
						else if(petition_index.get("id",itemId).getSingle() != null)
							pinned_items.add(petition_index.get("id",itemId).getSingle());
						else if(townhall_index.get("id",itemId).getSingle() != null)
							pinned_items.add(townhall_index.get("id",itemId).getSingle());
						else if(debate_index.get("id",itemId).getSingle() != null)
							pinned_items.add(debate_index.get("id",itemId).getSingle());
					}

					for(Relationship item_writtenByRel: user_node1.getRelationships(DynamicRelationshipType.withName("Article_Written_By")))
						total_items.add(item_writtenByRel.getOtherNode(user_node1));
					for(Relationship item_writtenByRel: user_node1.getRelationships(DynamicRelationshipType.withName("Event_Created_By")))
						total_items.add(item_writtenByRel.getOtherNode(user_node1));
					for(Relationship item_writtenByRel: user_node1.getRelationships(DynamicRelationshipType.withName("Petition_Written_By")))
						total_items.add(item_writtenByRel.getOtherNode(user_node1));
					for(Relationship item_writtenByRel: user_node1.getRelationships(DynamicRelationshipType.withName("Debate_Written_By")))
						total_items.add(item_writtenByRel.getOtherNode(user_node1));
					for(Relationship item_writtenByRel: user_node1.getRelationships(DynamicRelationshipType.withName("Townhall_Moderated_By")))
						total_items.add(item_writtenByRel.getOtherNode(user_node1));
					
					Collections.sort(total_items, TimeCreatedComparatorForNodes);
					
					ret = true;
				}
				else
					throw new RuntimeException("Unable to find user node for the given user_name:" + art_id);
			}
			else if("l".equalsIgnoreCase(tiles_type)) //loading / landing page
			{
				Node news_node = tiles_index.get("id","latest").getSingle();
				
				if(news_node != null)
				{
					tiles_ids = news_node.getProperty("value").toString();
					
					pinned_items.clear();
					
					for(String itemId : tiles_ids.split(","))
					{
						if(decommisioned_ids.contains(itemId))
							continue;
						
						if(total_items.size() == 15)
							break;
						
						if(article_index.get("id",itemId).getSingle() != null)
							total_items.add(article_index.get("id",itemId).getSingle());
						else if(event_index.get("id",itemId).getSingle() != null)
							total_items.add(event_index.get("id",itemId).getSingle());
						else if(petition_index.get("id",itemId).getSingle() != null)
							total_items.add(petition_index.get("id",itemId).getSingle());
						else if(townhall_index.get("id",itemId).getSingle() != null)
							total_items.add(townhall_index.get("id",itemId).getSingle());
						else if(debate_index.get("id",itemId).getSingle() != null)
							total_items.add(debate_index.get("id",itemId).getSingle());
					}
					ret = true;
				}
				else
					throw new RuntimeException("Unable to find featured node for latest tiles");
			}
			else if("".equalsIgnoreCase(tiles_type)) //normal category page
			{
				/* No need to fill this requset as there is no categories and topics 
				 * 
				 */
				
			/*	Node article_node = tiles_index.get("name","article").getSingle();
				Node event_node = tiles_index.get("name","event").getSingle();
				Node petition_node = tiles_index.get("name","petition").getSingle();
				Node debatet_node = tiles_index.get("name","debate").getSingle();
				Node townhall_node = tiles_index.get("name","townhall").getSingle();
				
				for(String itemId : pinned_tiles_ids.split(","))
				{
					if(pinned_items.size() == 1)
						break;
					if(decommisioned_ids.contains(itemId))
						continue;
					Node itemNode = null;
					if(article_index.get("id",itemId).getSingle() != null)
						itemNode = article_index.get("id",itemId).getSingle();
					else if(event_index.get("id",itemId).getSingle() != null)
						itemNode = event_index.get("id",itemId).getSingle();
					else if(petition_index.get("id",itemId).getSingle() != null)
						itemNode = petition_index.get("id",itemId).getSingle();
					else if(townhall_index.get("id",itemId).getSingle() != null)
						itemNode = townhall_index.get("id",itemId).getSingle();
					else if(debate_index.get("id",itemId).getSingle() != null)
						itemNode = debate_index.get("id",itemId).getSingle();
					
					if(itemNode != null && Integer.parseInt(itemNode.getProperty("space").toString())==0)
						pinned_items.add(itemNode);
				}

				for(Relationship item_BelongsToCat: cat_node.getRelationships(DynamicRelationshipType.withName("Belongs_To_Category")))
				{
					Node itemNode = item_BelongsToCat.getOtherNode(cat_node);
					if(itemNode != null && Integer.parseInt(itemNode.getProperty("space").toString())==0)
						total_items.add(itemNode);
				}
				for(Relationship item_BelongsToCat: cat_node.getRelationships(DynamicRelationshipType.withName("Belongs_To_Event_Category")))
				{
					Node itemNode = item_BelongsToCat.getOtherNode(cat_node);
					if(itemNode != null && Integer.parseInt(itemNode.getProperty("space").toString())==0)
						total_items.add(itemNode);
				}
				for(Relationship item_BelongsToCat: cat_node.getRelationships(DynamicRelationshipType.withName("Belongs_To_Petition_Category")))
				{
					Node itemNode = item_BelongsToCat.getOtherNode(cat_node);
					if(itemNode != null && Integer.parseInt(itemNode.getProperty("space").toString())==0)
						total_items.add(itemNode);
				}
				for(Relationship item_BelongsToCat: cat_node.getRelationships(DynamicRelationshipType.withName("Belongs_To_Debate_Category")))
				{
					Node itemNode = item_BelongsToCat.getOtherNode(cat_node);
					if(itemNode != null && Integer.parseInt(itemNode.getProperty("space").toString())==0)
						total_items.add(itemNode);
				}
				for(Relationship item_BelongsToCat: cat_node.getRelationships(DynamicRelationshipType.withName("Belongs_To_Townhall_Category")))
				{
					Node itemNode = item_BelongsToCat.getOtherNode(cat_node);
					if(itemNode != null && Integer.parseInt(itemNode.getProperty("space").toString())==0)
						total_items.add(itemNode);
				}
				ret = true; */
				
			}
			else{ //either trending or personalized
				
					if("p".equalsIgnoreCase(tiles_type)) //personalized
					{
						if(user_node.hasRelationship(DynamicRelationshipType.withName("Belongs_To_Location"),Direction.OUTGOING))
						{
							Node loc = user_node.getSingleRelationship(DynamicRelationshipType.withName("Belongs_To_Location"),Direction.OUTGOING).getOtherNode(user_node);
							tiles_ids = loc.getProperty("tiles").toString();
							Node user_tiles_node = userTiles_index.get("id",user_name).getSingle();
							String perso_tiles = user_tiles_node.getProperty("news_Personalized").toString();

							for(String itemId: tiles_ids.split(","))
							{
								if(decommisioned_ids.contains(itemId))
									continue;
								Node itemNode = null;
								if(article_index.get("id",itemId).getSingle() != null)
									itemNode = article_index.get("id",itemId).getSingle();
								else if(event_index.get("id",itemId).getSingle() != null)
									itemNode = event_index.get("id",itemId).getSingle();
								else if(petition_index.get("id",itemId).getSingle() != null)
									itemNode = petition_index.get("id",itemId).getSingle();
								else if(townhall_index.get("id",itemId).getSingle() != null)
									itemNode = townhall_index.get("id",itemId).getSingle();
								else if(debate_index.get("id",itemId).getSingle() != null)
									itemNode = debate_index.get("id",itemId).getSingle();
								
								if(itemNode != null && Integer.parseInt(itemNode.getProperty("space").toString())==0 && Integer.parseInt(itemNode.getProperty("skip_from_special_tiles").toString())==0)
									total_items.add(itemNode);
							}
							
							for(String itemId: perso_tiles.split(","))
							{
								if(decommisioned_ids.contains(itemId))
									continue;
								Node itemNode = null;
								if(article_index.get("id",itemId).getSingle() != null)
									itemNode = article_index.get("id",itemId).getSingle();
								else if(event_index.get("id",itemId).getSingle() != null)
									itemNode = event_index.get("id",itemId).getSingle();
								else if(petition_index.get("id",itemId).getSingle() != null)
									itemNode = petition_index.get("id",itemId).getSingle();
								else if(townhall_index.get("id",itemId).getSingle() != null)
									itemNode = townhall_index.get("id",itemId).getSingle();
								else if(debate_index.get("id",itemId).getSingle() != null)
									itemNode = debate_index.get("id",itemId).getSingle();
								
								if(itemNode != null && Integer.parseInt(itemNode.getProperty("space").toString())==0 && Integer.parseInt(itemNode.getProperty("skip_from_special_tiles").toString())==0)
									total_items.add(itemNode);
							}
							
							
							ret = true;
							
						}
					}
					else  //trending
					{
						Node news_node = tiles_index.get("id", "trending").getSingle();
						tiles_ids = news_node.getProperty("value").toString();

						for(String itemId: tiles_ids.split(","))
						{
							if(decommisioned_ids.contains(itemId))
								continue;
							Node itemNode = null;
							if(article_index.get("id",itemId).getSingle() != null)
								itemNode = article_index.get("id",itemId).getSingle();
							else if(event_index.get("id",itemId).getSingle() != null)
								itemNode = event_index.get("id",itemId).getSingle();
							else if(petition_index.get("id",itemId).getSingle() != null)
								itemNode = petition_index.get("id",itemId).getSingle();
							else if(townhall_index.get("id",itemId).getSingle() != null)
								itemNode = townhall_index.get("id",itemId).getSingle();
							else if(debate_index.get("id",itemId).getSingle() != null)
								itemNode = debate_index.get("id",itemId).getSingle();
							
							if(itemNode != null)
								total_items.add(itemNode);
						}
						ret = true;
					}
				
			}

			Collections.sort(total_items, TimeCreatedComparatorForNodes);
			total_items.removeAll(pinned_items);
			pinned_items.addAll(total_items);
			
			pinned_items.trimToSize();
			int totalLen = pinned_items.size();
			int fromIndex = prev_cnt;
			int toIndex = totalLen < count + prev_cnt ? totalLen : count+prev_cnt;
			if(fromIndex > toIndex)
				toIndex = fromIndex;
			
			List<Node> actualRes = pinned_items.subList(fromIndex, toIndex);
			
			for(Node item: actualRes)
			{
				if(item.getProperty("__CLASS__").toString().equals("Saddahaq.article"))
					resJSON.put(getArticleJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.event"))
					resJSON.put(getEventJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.petition"))
					resJSON.put(getPetitionJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.townhall"))
					resJSON.put(getTownhallJSONForTile(item, false, user_node));
				else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.debate"))
					resJSON.put(getDebateJSONForTile(item, false, user_node));
			}
		    
		  //  ret = true;  // changing the return value to TRUE if everything is fine
			tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ get_tiles('"+user_name+"','"+cat+"',"+count+","+prev_cnt+",'"+art_id+"','"+tiles_type+"')");
			System.out.println("Something went wrong, while reading items from get_tiles  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		if(ret == false)
			return "";
		else
			return resJSON.toString();
	}

	public String get_tiles_temp(String user_name, String cat, int count,
			int prev_cnt, String art_id, String tiles_type) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_elections_home(String user_name, String filter_type,
			String filter_value, int count, int prev_cnt) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_elections_more(String user_name, String filter_type,
			String filter_value, String item_type, int count, int prev_cnt)
			throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public boolean delete_article(String id) throws TException {
		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> articleIndex = graphDb.index().forNodes("article");
			Index<Node> userIndex = graphDb.index().forNodes("user");
			Index<Node> userWeightIndex = graphDb.index().forNodes("user_weight");
			Index<Node> commentIndex = graphDb.index().forNodes("comment");
			Index<Node> hashWeightIndex = graphDb.index().forNodes("hash_weight");
			Index<Node> articleContentIndex = graphDb.index().forNodes("article_content");
			Index<Node> articleTitleIndex = graphDb.index().forNodes("article_title");
			Index<Node> articleWeightIndex = graphDb.index().forNodes("article_weight");
			IndexHits<Node> articles = articleIndex.query("id", id);
			while(articles.hasNext())
			{
				Node article = articles.next();
				if(article.hasRelationship(DynamicRelationshipType.withName("item_trending_weights")))
				{
					Relationship itwRel = article.getSingleRelationship(DynamicRelationshipType.withName("item_trending_weights"), Direction.OUTGOING);
					Node itwNode = itwRel.getOtherNode(article);
					itwRel.delete();
					itwNode.delete();
				}
				
				//unpin the article
				pin_item("A",id,"f","");
				pin_item("A",id,"o","");
				
				IndexHits<Node> totalUsers = userIndex.query("id","*");
				while(totalUsers.hasNext())
				{
					Node eachUser = totalUsers.next();
					if(eachUser.getProperty("pins").toString().contains(id))
						pin_item("A",id,"u",eachUser.getProperty("user_name").toString());
				}
				
				for(Relationship artTagToSpaceRel : article.getRelationships(DynamicRelationshipType.withName("Article_Tagged_To_Space")))
					pin_item("A",id,"s",artTagToSpaceRel.getOtherNode(article).getProperty("space_id").toString());
				
				exclusive_article(id,"");
				
				Relationship authNodeRel = article.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING);
				Node authNode = authNodeRel.getOtherNode(article);
				int del_wt = Integer.parseInt(authNodeRel.getProperty("out_weight").toString());
				int author_wt = Integer.parseInt(authNode.getProperty("weight").toString());
				int art_wt = Integer.parseInt(article.getProperty("weight").toString());
				
				authNode.setProperty("weight",author_wt-(art_wt+10-del_wt));
				userWeightIndex.remove(authNode);
				userWeightIndex.add(authNode,"weight",new ValueContext( author_wt-(art_wt+10-del_wt) ).indexNumeric());
				
				for(Relationship eachArtRel : article.getRelationships())
				{
					Node otherNode = eachArtRel.getOtherNode(article);
					if(otherNode.getProperty("__CLASS__").equals("Saddahaq.commment"))
					{
						for(Relationship tempRel : otherNode.getRelationships())
							tempRel.delete();
						commentIndex.remove(otherNode);
						otherNode.delete();
					}
					else if(otherNode.getProperty("__CLASS__").equals("Saddahaq.location"))
					{
						StringBuffer locTiles = new StringBuffer();
						for(String eachItemId : otherNode.getProperty("tiles").toString().split(","))
							if(!eachItemId.equals(id))
								locTiles.append(","+eachItemId);
						if(locTiles.length() > 1)
							locTiles.replace(0, 1, "");
						otherNode.setProperty("tiles", locTiles.toString());
						eachArtRel.delete();
					}
					else if(otherNode.getProperty("__CLASS__").equals("Saddahaq.poll"))
					{
						otherNode.getSingleRelationship(DynamicRelationshipType.withName("Poll_App_Of"),Direction.OUTGOING).delete();
						for(Relationship votedPollRel: otherNode.getRelationships(DynamicRelationshipType.withName("Voted_To_Poll")))
							votedPollRel.delete();
						otherNode.delete();
					}
					else if(otherNode.getProperty("__CLASS__").equals("Saddahaq.sub_category"))
					{
						int artWt = 10;
						if(eachArtRel.getType().name().equals("Tag_Of_Article"))
							artWt = 10;
						else artWt = art_wt;
						int hashWt = Integer.parseInt(otherNode.getProperty("weight").toString());
						otherNode.setProperty("weight",(hashWt - 10));
						hashWeightIndex.remove(otherNode);
						hashWeightIndex.add(otherNode,"weight",new ValueContext( hashWt - artWt ).indexNumeric());
						eachArtRel.delete();
					}
				}	
				
				//delete any pending relations
				for(Relationship eachArtRel : article.getRelationships())
					eachArtRel.delete();
				//remove from featured, trending, latest, headlines, user profile
				
				articleIndex.remove(article);
				articleContentIndex.remove(article);
				articleTitleIndex.remove(article);
				articleWeightIndex.remove(article);
				article.delete();
			}
			tx.success();
			ret = true;
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ delete_article('"+id+"')");
			System.out.println("Something went wrong, while deleting artilcle from delete_article()  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public boolean delete_space(String id) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean delete_poll(String id) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public String get_userfeed(String feed_type) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_itemfeed(String feed_type) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_trends(String cat) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String suggestions(String item_type, String item_id, String content,
			String cat, String hashtags, int count, int prev_cnt)
			throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String suggestions_morenames(String item_type, String item_id)
			throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String view_suggestions(String item_type, String item_id,
			String a_ids, int count, String user_name, String hashtags)
			throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String cf_suggestions(String item_type, String item_id,
			String user_name) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String hashtag_suggestions(String item_type, String item_id,
			String content, String cat) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String search(String user_name, String content, int cnt,
			int prev_cnt, String item_type) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public boolean add_feed(String item_type, String item_id) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean pin_item(String item_type, String item_id, String pin_type,
			String pin_item_to) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean exclusive_article(String a_id, String cat) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public String fetch_pin(String item_type, String a_id) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String exclusive_article_category(String a_id) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_user_tiles(String user_name) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public boolean create_event(String user_name, String e_id,
			String e_title_id, String e_title, String e_content,
			int e_date_time, int e_date_time_closing, int e_limit,
			String e_location, String e_cat, String e_subcat,
			String e_hashtags, String e_users, int e_time_created, String a_id,
			String e_summary, int is_edit, String e_fut_image, int is_closed,
			String lang) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean report_spam(String item_type, String item_id,
			String user_name, int time) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean delete_event(String id) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean event_response(String id, String user_name, int time)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean event_invite(String id, String user_name, String users,
			int time) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean event_notify(String user_name, String id, String content,
			int time) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean event_changeresponse(String id, String user_name, int time)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public String get_events_category(String user_name, String category)
			throws TException {
		System.out.println("No categories");
		return "";
	}

	public String get_leftpane(String user_name, String item_type,
			String content, String category) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_leftpane_more(String user_name, String item_type,
			String item_name, String content, String category, int count,
			int prev_cnt) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_more_events_category(String user_name, String category,
			int event_type) throws TException {
		System.out.println("No categories");
		return "";
	}

	public String get_more_events(String user_name, String category)
			throws TException {
		System.out.println("No categories");
		return "";
	}

	public String get_more_petitions() throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_more_debates() throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_more_townhalls() throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_all_events(String user_name, String category, int count,
			int prev_cnt) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_all_petitions(int count, int prev_cnt) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_all_debates(int count, int prev_cnt) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_all_townhalls(int count, int prev_cnt) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public Lock aquireWriteLock(Transaction tx, String lockNodeName) throws Exception {
		Index<Node> lockNodeIndex = graphDb.index().forNodes( "lock" );
		Node tobeLockedNode = lockNodeIndex.get( "name", lockNodeName ).getSingle();
		if(tobeLockedNode == null)
	      throw new RuntimeException("Locking node for "+lockNodeName+" not found, unbale to synchronize the call.");
		return tx.acquireWriteLock(tobeLockedNode);  //lock simultaneous execution of create_comment to avoid duplicate comment creation
	}
	
	public boolean create_comment(String c_itemid, String c_itemgroup,
			String c_id, String c_content, String c_users, int c_time_created,
			String user_name) throws TException {

		boolean ret = false;
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx, "comment");
			String index_type = indexFullName.get(c_itemgroup);

			Index<Node> event_weight_index = graphDb.index().forNodes("event_weight");
			Index<Node> hash_weight_index = graphDb.index().forNodes("hash_weight");
			Index<Node> user_weight_index = graphDb.index().forNodes("user_weight");
			Index<Node> comment_index = graphDb.index().forNodes("comment");
			Index<Node> article_weight_index = graphDb.index().forNodes("article_weight");
			Index<Node> p_weight_index = graphDb.index().forNodes("petition_weight");
			Index<Node> UserIndex = graphDb.index().forNodes("user");
			Index<Node> ParentIndex = graphDb.index().forNodes(index_type.toLowerCase());
				     
			Node user_node = UserIndex.get("id",user_name).getSingle();
			Node comment_node = comment_index.get("id",c_id).getSingle();
			Node parent_node = ParentIndex.get("id",c_itemid).getSingle();
			ArrayList<Node> hash_nodes = new ArrayList<Node>();
			
			if(comment_node == null && user_node != null && parent_node != null)
			{
				
				user_node.setProperty("last_seen",c_time_created);
			    int u_wt = Integer.parseInt(user_node.getProperty("weight").toString());
			    int c_wt = Math.round(((float)u_wt)/300);
			    
			    comment_node = comment(c_id,c_content,c_time_created,c_wt,0);
			    comment_index.add(comment_node,"id",c_id);
			    user_node.setProperty("weight",u_wt+1);
			    user_weight_index.remove(user_node);
			    user_weight_index.add(user_node,"weight",new ValueContext( u_wt+1 ).indexNumeric());
			    
			    Relationship rel = comment_node.createRelationshipTo(user_node, DynamicRelationshipType.withName("Comment_Written_By"));
			     
			    rel.setProperty("time", c_time_created);
			    rel.setProperty("in_weight", 1);
			    rel.setProperty("out_weight", c_wt);
			
			    int p_wt = Integer.parseInt(parent_node.getProperty("weight").toString());
			    parent_node.setProperty("weight",p_wt+1);
			     
			    if(index_type.equalsIgnoreCase("article"))
			    {
			    	for(Relationship temp_rel : parent_node.getRelationships(Direction.OUTGOING, DynamicRelationshipType.withName("Belongs_To_Subcategory_Article")))
			    		hash_nodes.add(temp_rel.getOtherNode(parent_node));
			    	article_weight_index.remove(parent_node);
				    article_weight_index.add(parent_node,"weight",new ValueContext( p_wt+1 ).indexNumeric());
				    Relationship commToArtRel = comment_node.createRelationshipTo(parent_node, DynamicRelationshipType.withName("Comment_To_Article"));
				    commToArtRel.setProperty("time", c_time_created);
				    commToArtRel.setProperty("in_weight", 1);
				    commToArtRel.setProperty("out_weight", 0);
				    Node art_author_node = parent_node.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getEndNode();
				    int au_wt = Integer.parseInt(art_author_node.getProperty("weight").toString());
				    art_author_node.setProperty("weight",au_wt+1);
				    user_weight_index.remove(art_author_node);
				    user_weight_index.add(art_author_node,"weight",new ValueContext( au_wt+1 ).indexNumeric());
			    }
			    else if(index_type.equalsIgnoreCase("event"))
			    {
			    	for(Relationship temp_rel : parent_node.getRelationships(Direction.OUTGOING, DynamicRelationshipType.withName("Belongs_To_Subcategory_Event")))
			    		hash_nodes.add(temp_rel.getOtherNode(parent_node));
			    	event_weight_index.remove(parent_node);
				    event_weight_index.add(parent_node,"weight",new ValueContext( p_wt+1 ).indexNumeric());
				    Relationship commToEventRel = comment_node.createRelationshipTo(parent_node, DynamicRelationshipType.withName("Comment_To_Event"));
				    commToEventRel.setProperty("time", c_time_created);
				    commToEventRel.setProperty("in_weight", 1);
				    commToEventRel.setProperty("out_weight", 0);
				    Node event_author_node = parent_node.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getEndNode();
				    int au_wt = Integer.parseInt(event_author_node.getProperty("weight").toString());
				    event_author_node.setProperty("weight",au_wt+1);
				    user_weight_index.remove(event_author_node);
				    user_weight_index.add(event_author_node,"weight",new ValueContext( au_wt+1 ).indexNumeric());
			    }
			    else if(index_type.equalsIgnoreCase("petition"))
			    {
			    	for(Relationship temp_rel : parent_node.getRelationships(Direction.OUTGOING, DynamicRelationshipType.withName("Belongs_To_Subcategory_Petition")))
			    		hash_nodes.add(temp_rel.getOtherNode(parent_node));
			    	p_weight_index.remove(parent_node);
				    p_weight_index.add(parent_node,"weight",new ValueContext( p_wt+1 ).indexNumeric());
				    Relationship commToPetitionRel = comment_node.createRelationshipTo(parent_node, DynamicRelationshipType.withName("Comment_To_Petition"));
				    commToPetitionRel.setProperty("time", c_time_created);
				    commToPetitionRel.setProperty("in_weight", 1);
				    commToPetitionRel.setProperty("out_weight", 0);
				    Node petition_author_node = parent_node.getSingleRelationship(DynamicRelationshipType.withName("Petition_Writtend_By"),Direction.OUTGOING).getEndNode();
				    int au_wt = Integer.parseInt(petition_author_node.getProperty("weight").toString());
				    petition_author_node.setProperty("weight",au_wt+1);
				    user_weight_index.remove(petition_author_node);
				    user_weight_index.add(petition_author_node,"weight",new ValueContext( au_wt+1 ).indexNumeric());
			    }
			    else if(index_type.equalsIgnoreCase("comment"))
			    {
			    	Relationship cmnt_to_cmnt_rel = comment_node.createRelationshipTo(parent_node, DynamicRelationshipType.withName("Comment_To_Comment"));
			    	cmnt_to_cmnt_rel.setProperty("time", c_time_created);
			    	cmnt_to_cmnt_rel.setProperty("in_weight", 1);
			    	cmnt_to_cmnt_rel.setProperty("out_weight", 0);
			    	
			    	if(parent_node.getSingleRelationship(DynamicRelationshipType.withName("Comment_To_Article"),Direction.OUTGOING) != null)
			    	{
			    		Node art_node = parent_node.getSingleRelationship(DynamicRelationshipType.withName("Comment_To_Article"),Direction.OUTGOING).getEndNode();
			    		for(Relationship temp_rel: art_node.getRelationships(Direction.OUTGOING,DynamicRelationshipType.withName("Belongs_To_Subcategory_Article")))
			    			hash_nodes.add(temp_rel.getOtherNode(art_node));
			    		Node author_node = art_node.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getEndNode();
			    		int a_wt = Integer.parseInt(art_node.getProperty("weight").toString());
			    		art_node.setProperty("weight",a_wt+1);
			    		article_weight_index.remove(art_node);
			    		article_weight_index.add(art_node,"weight",new ValueContext( a_wt+1 ).indexNumeric());
			    		Relationship cmntToArtRel = comment_node.createRelationshipTo(art_node, DynamicRelationshipType.withName("Comment_To_Article"));
			    		cmntToArtRel.setProperty("time", c_time_created);
			    		cmntToArtRel.setProperty("in_weight", 1);
			    		cmntToArtRel.setProperty("out_weight", 0);
			    		          
			    		int au_wt = Integer.parseInt(author_node.getProperty("weight").toString());
			    		author_node.setProperty("weight",au_wt+1);
			    		user_weight_index.remove(author_node);
			    		user_weight_index.add(author_node,"weight",new ValueContext( au_wt+1 ).indexNumeric());
			    	}
			    	else if(parent_node.getSingleRelationship(DynamicRelationshipType.withName("Comment_To_Event"),Direction.OUTGOING) != null)
			    	{
			    		Node event_node = parent_node.getSingleRelationship(DynamicRelationshipType.withName("Comment_To_Event"),Direction.OUTGOING).getEndNode();
			    		for(Relationship temp_rel: event_node.getRelationships(Direction.OUTGOING,DynamicRelationshipType.withName("Belongs_To_Subcategory_Event")))
			    			hash_nodes.add(temp_rel.getOtherNode(event_node));
			    		Node author_node = event_node.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getEndNode();
			    		int e_wt = Integer.parseInt(event_node.getProperty("weight").toString());
			    		event_node.setProperty("weight",e_wt+1);
			    		event_weight_index.remove(event_node);
			    		event_weight_index.add(event_node,"weight",new ValueContext( e_wt+1 ).indexNumeric());
			    		
			    		Relationship cmntToEvntRel = comment_node.createRelationshipTo(event_node, DynamicRelationshipType.withName("Comment_To_Event"));
			    		cmntToEvntRel.setProperty("time", c_time_created);
			    		cmntToEvntRel.setProperty("in_weight", 1);
			    		cmntToEvntRel.setProperty("out_weight", 0);
			    		          
			    		int au_wt = Integer.parseInt(author_node.getProperty("weight").toString());
			    		author_node.setProperty("weight",au_wt+1);
			    		user_weight_index.remove(author_node);
			    		user_weight_index.add(author_node,"weight",new ValueContext( au_wt+1 ).indexNumeric());
			    	}
			    	else
			    	{
			    		Node p_node = parent_node.getSingleRelationship(DynamicRelationshipType.withName("Comment_To_Petition"),Direction.OUTGOING).getEndNode();
			    		for(Relationship temp_rel : p_node.getRelationships(Direction.OUTGOING,DynamicRelationshipType.withName("Belongs_To_Subcategory_Petition")))
			    			hash_nodes.add(temp_rel.getOtherNode(p_node));
			    		Node author_node = p_node.getSingleRelationship(DynamicRelationshipType.withName("Petition_Written_By"),Direction.OUTGOING).getEndNode();
			    		int pet_wt = Integer.parseInt(p_node.getProperty("weight").toString());
			    		p_node.setProperty("weight",pet_wt+1);
			    		p_weight_index.remove(p_node);
			    		p_weight_index.add(p_node,"weight",new ValueContext( pet_wt+1 ).indexNumeric());

			    		Relationship cmntToPetRel = comment_node.createRelationshipTo(p_node, DynamicRelationshipType.withName("Comment_To_Petition"));
			    		cmntToPetRel.setProperty("time", c_time_created);
			    		cmntToPetRel.setProperty("in_weight", 1);
			    		cmntToPetRel.setProperty("out_weight", 0);
			    		          
			    		int au_wt = Integer.parseInt(author_node.getProperty("weight").toString());
			    		author_node.setProperty("weight",au_wt+1);
			    		user_weight_index.remove(author_node);
			    		user_weight_index.add(author_node,"weight",new ValueContext( au_wt+1 ).indexNumeric());
			    	}
			    }
			     
			    for(Node hashNode: hash_nodes)
			    {
			    	int h_wt = Integer.parseInt(hashNode.getProperty("weight").toString());
			    	hashNode.setProperty("weight",h_wt+1);
			    	hash_weight_index.remove(hashNode);
			    	hash_weight_index.add(hashNode,"weight",new ValueContext( h_wt+1 ).indexNumeric());
			    }
			     
		        if(c_users != null && !"".equals(c_users))
		        {
		        	String[] c_users_list = c_users.split(",");
		        	
		            for(String c_user: c_users_list)
		            	if(!c_user.equals(""))
		            	{
			              Node user_node1 = UserIndex.get("id",c_user).getSingle();
			              if(user_node1 != null)
			              {
			            	  Relationship rel1 = user_node1.createRelationshipTo(comment_node, DynamicRelationshipType.withName("User_Of_Comment"));
			            	  rel1.setProperty("time", c_time_created);
			              }
		            	}
			    }
		        ret = true;
			}
			
			if(ret == true) tx.success();
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ create_comment('"+c_itemid+"','"+c_itemgroup+"','"+c_id+"','"+c_content+"','"+c_users+"',"+c_time_created+",'"+user_name+"')");
			System.out.println("Something went wrong, while creating comment from create_comment  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}
		
		return ret;
	}

	public boolean create_comment_own(String c_itemid, String c_itemgroup,
			String c_id, String c_content, String c_users, int c_time_created,
			String user_name) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean edit_comment(String user_name, String c_id,
			String c_content, String c_users, int c_time_created)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean delete_comment(String id) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean comment_spam(String id, String user_name, int time)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public String get_all_comments(String user_name, String c_itemtype,
			String c_itemid) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public String get_comments(String user_name, String c_id) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public boolean debate_townhall_suggestion(String item_type,
			String user_name, String item_id, String item_topic,
			String item_celebrity, int item_time_created) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean debate_townhall_suggestion_voteup(String item_type,
			String user_name, String item_id) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean create_petition(String p_type, String user_name,
			String p_id, String p_title, String p_title_id, String p_content,
			String p_img_url, String p_to, int p_target, int p_count,
			int p_time_created, int p_end_date, String p_subcat,
			String p_hashtags, String p_cat, int is_edit, int is_closed,
			String lang) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean sign_petition(String user_name, String p_id, int time)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean update_petition_signs(String p_id, int signs)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean user_signed_petition(String user_name, String p_id)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean create_debate(String user_name, String d_id, String d_title,
			String d_title_id, String d_content, String d_img_url,
			String d_criteria, int d_duration, int d_date, int d_time_created,
			String d_subcat, String d_hashtags, int is_edit, int is_closed,
			String lang) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean debate_action(String user_name, String d_id,
			String action_type, String qtn_id, String qtn_content, int time)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean debate_comment(String d_id, String user_name,
			String comment, int time) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean debate_participate(String d_id, String user_name,
			String grp_name, String message, int time) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean debate_shortlist_guests(String d_id, String grp_name,
			String user_names) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean debate_change_moderator(String d_id, String d_moderators,
			int time) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean create_townhall(String user_name, String t_id,
			String t_title, String t_title_id, String t_content,
			String t_img_url, int t_date, int t_duration, int t_time_created,
			String t_celeb, String t_moderators, String t_subcat,
			String t_hashtags, int is_edit, int is_closed, String lang)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean townhall_action(String user_name, String t_id,
			String action_type, String qtn_id, String qtn_content, int time)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean townhall_comment(String t_id, String user_name,
			String comment, int time) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean townhall_approve_question(String t_id, String qtn_id)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean townhall_change_moderator(String t_id, String t_moderators,
			int time) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean delete_petition(String id) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean delete_debate(String id) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean delete_townhall(String id) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean delete_debate_townhall_suggestion(String item_type, String id)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public void jar_check() throws TException {
		// TODO Auto-generated method stub
		
	}

	public void jar_shutdown() throws TException {
		try{
			  System.out.println("Got the request to shut_down");
			 Timer timer  = new Timer ();
					TimerTask hourlyTask = new TimerTask () {
					    @Override
					    public void run () {
					      System.out.println("Jar will terminate in 1 minute");
					    	System.exit(0);
					    }
					};
					timer.schedule (hourlyTask, 60000);
			}
			catch(Exception ex) {
			      System.out.println("Something went wrong, while jar_shutdown :"+ex.getMessage());
			      ex.printStackTrace();
			}
		
	}

	public void tweet_sentiment(String hashtags) throws TException {
		// TODO Auto-generated method stub
		
	}

	public String sentiment_analysis(String hashtag) throws TException {
		// TODO Auto-generated method stub
		return "";
	}

	public boolean create_space(String user_name, String space_id,
			String space_title_id, String space_title, String space_tagline,
			String space_fut_image, int space_time_created, int is_edit,
			int is_closed) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean change_category_name(String old_name, String new_name)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean remove_duplicate_category(String category_name)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean delete_dangling_spaces() throws TException {
		// TODO Auto-generated method stub
		return false;
	}


	public void add_firstname_index()
	{
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> posIndex = graphDb.index().forNodes("pos");
			Node posNode = posIndex.get("id", "proper_nouns").getSingle();
			if(posNode == null)
				posIndex.add(pos("proper_nouns",100), "id", "proper_nouns");
			else
				posNode.setProperty("last_update",100);
			tx.success();
			
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ add_firstname_index()");
			System.out.println("Something went wrong, while updating proper nouns node from add_firstname_index  :"+ex.getMessage());
		
		}
		finally{
			
		}
	}
	
	// This function uses the cities.txt to create all the location nodes ( run only once )
	public void location_store()
	{

		FileInputStream fis = null;
		BufferedReader br = null;
		
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> locationIndex = graphDb.index().forNodes("location");
			if(locationIndex.get("id", "andhra pradesh").getSingle() == null)
			{
				fis = new FileInputStream("");
				br = new BufferedReader(new InputStreamReader(fis));
				StringBuffer allCities = new StringBuffer();
				String line = null;
				while ((line = br.readLine()) != null) {
					//System.out.println(line);
					String[] sl = line.split("\t");
					String item = sl[0].replace("*","");
					if(item.contains(", "))
				      {
				        item = item.split(", ")[0];
				      }
					allCities.append(","+item);
					String state = sl[1].toLowerCase();
					Node location_node = locationIndex.get("id",state).getSingle();
					if(location_node != null)
						location_node.setProperty("cities",item+","+location_node.getProperty("cities").toString());
					else locationIndex.add(location(state,item,""),"id",state);
				}
				
				if(allCities.length() > 1)
					allCities.replace(0,1,"");
				locationIndex.add(location("all",allCities.toString(), ""), "id", "all");
				System.out.println("Locations Added");
			}
			tx.success();
			
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ add_firstname_index()");
			System.out.println("Something went wrong, while updating proper nouns node from add_firstname_index  :"+ex.getMessage());
		
		}
		finally{
			try{
				if(br != null)
					br.close();
				if(fis != null)
					fis.close();
			}catch(Exception e){}
		}
	}
	
	public void keyword_store()
	{
		// as categories and topics are deleting, no need of keyword_store
	}
	
	public void add_exclusive_property()
	{
		try(Transaction tx = graphDb.beginTx()){

				Index<Node> tiles_index = graphDb.index().forNodes("tiles");
				Index<Node> featured_tiles_index = graphDb.index().forNodes("featured_tiles");
				Index<Node> cat_index = graphDb.index().forNodes("category");
				Index<Node> topic_index = graphDb.index().forNodes("topic");
				Index<Node> headlines_index = graphDb.index().forNodes("headlines");

				String trendingTilesIds = "";
				String featuredTilesIds = "";
				String featuredPinnedTilesIds = "";
				String exclusivePinnedTilesIds = "";
				String headlineTilesIds = "";
				String latestTilesIds = "";
				
				//trending tiles
				Node trendingTilesNode = tiles_index.get("id","all").getSingle();
				if(trendingTilesNode != null)
					trendingTilesIds = trendingTilesNode.getProperty("value").toString();
				
				//featured tiles
				Node featuredTilesNode = featured_tiles_index.get("id","all").getSingle();
				if(featuredTilesNode != null)
					featuredTilesIds = featuredTilesNode.getProperty("value").toString();
				
				//pin tiles
			    Node pinTilesNode = cat_index.get("name","all").getSingle();
			    if(pinTilesNode != null)
			    {
			    	featuredPinnedTilesIds = pinTilesNode.getProperty("pins").toString();
			        exclusivePinnedTilesIds = pinTilesNode.getProperty("exclusive").toString();
			    }
			    
			    //headlines tiles
			    Node headlinesTilesNode = headlines_index.get("id","all").getSingle();
			    if(headlinesTilesNode != null)
			    	headlineTilesIds = headlinesTilesNode.getProperty("value").toString();
			      
			    //latest
			    Node latestTilesNode = featured_tiles_index.get("id","latest").getSingle();
			    if(latestTilesNode != null)
			    	latestTilesIds = latestTilesNode.getProperty("value").toString();
			    
			    //delete all nodes from these indexes
			    for(Node each : tiles_index.query("id","*"))
			      {
			        for(Relationship eachRel : each.getRelationships())
			          eachRel.delete();
			        tiles_index.remove(each);
			        each.delete();
			      }
			    for(Node each : featured_tiles_index.query("id","*"))
			      {
			        for(Relationship eachRel : each.getRelationships())
			          eachRel.delete();
			        tiles_index.remove(each);
			        each.delete();
			      }
			    for(Node each : headlines_index.query("id","*"))
			      {
			        for(Relationship eachRel : each.getRelationships())
			          eachRel.delete();
			        tiles_index.remove(each);
			        each.delete();
			      }
			    for(Node each : cat_index.query("id","*"))
			      {
			        for(Relationship eachRel : each.getRelationships())
			          eachRel.delete();
			        tiles_index.remove(each);
			        each.delete();
			      }
			    for(Node each : topic_index.query("id","*"))
			      {
			        for(Relationship eachRel : each.getRelationships())
			          eachRel.delete();
			        tiles_index.remove(each);
			        each.delete();
			      }
			    
			  //then create new nodes and indexes
			    
			    Node new_trending_tiles_node = tiles_index.get("id","trending").getSingle();
			    if(new_trending_tiles_node == null)
			    {
			    	new_trending_tiles_node = tiles("trending",trendingTilesIds);
			    	tiles_index.add(new_trending_tiles_node, "id","trending");
			    }
			    
			    Node new_featured_tiles_node = tiles_index.get("id","featured").getSingle();
			    if(new_featured_tiles_node == null)
			    {
			    	new_featured_tiles_node = tiles("featured",featuredTilesIds);
			    	tiles_index.add(new_featured_tiles_node, "id","featured");
			    }
			    
			    Node new_feature_pinned_tiles_node = tiles_index.get("id","feature_pinned").getSingle();
			    if(new_feature_pinned_tiles_node == null)
			    {
			    	new_feature_pinned_tiles_node = tiles("feature_pinned",featuredPinnedTilesIds);
			    	tiles_index.add(new_feature_pinned_tiles_node, "id","feature_pinned");
			    }
			    			    
			    Node new_exclusive_pinned_tiles_node = tiles_index.get("id","exclusive_pinned").getSingle();
			    if(new_exclusive_pinned_tiles_node == null)
			    {
			    	new_exclusive_pinned_tiles_node = tiles("exclusive_pinned",exclusivePinnedTilesIds);
			    	tiles_index.add(new_exclusive_pinned_tiles_node, "id","exclusive_pinned");
			    }
			    
			    Node new_headlines_tiles_node = tiles_index.get("id","headlines").getSingle();
			    if(new_headlines_tiles_node == null)
			    {
			    	new_headlines_tiles_node = tiles("headlines",headlineTilesIds);
			    	tiles_index.add(new_headlines_tiles_node, "id","headlines");
			    }
			    
			    Node new_latest_tiles_node = tiles_index.get("id","latest").getSingle();
			    if(new_latest_tiles_node == null)
			    {
			    	new_latest_tiles_node = tiles("latest",latestTilesIds);
			    	tiles_index.add(new_latest_tiles_node, "id","latest");
			    }	    
			    
			    tx.success();
			    
			}
			catch(Exception ex) {
			      System.out.println("Something went wrong, while add_exclusive_property :"+ex.getMessage());
			      ex.printStackTrace();
			}
		finally{
		}
	}
		
	public void add_neo4j_lock_nodes()
	{
		String[] lockNames = {"user", "article", "event", "petition", "townhall", "debate", "space", "hashtag", "comment"};
		
		try (Transaction tx = graphDb.beginTx())
		{
			Index<Node> lockNodeIndex = graphDb.index().forNodes( "lock" );
				
			//create lock nodes for each lock name //check whether lock node is already created or not
			for(String lockName: lockNames)
				if(lockNodeIndex.get("name", lockName).getSingle() == null){ //create node if and only if there no locknode with given name
					Node lockNode = graphDb.createNode();  //creating a node
					lockNode.setProperty( "name", lockName ); //attach name to lock node
					lockNodeIndex.add( lockNode, "name", lockName ); //attach node to lock node index to retrieve later
				}
			
			tx.success();
		}
		catch(Exception e){
			System.out.println("Exception @ add_neo4j_lock_nodes()");
			System.out.println("Failed to create lock nodes : " + e.getMessage());}
		finally{}
	}

	//will be called, for every 15 min by background thread
	public void calc_views()
	{
		try (Transaction tx = graphDb.beginTx())
		{
			String[] indexNames = {"article","event","petition"};
			int t = (int)(System.currentTimeMillis()/1000) - 14400;  //4 hours ago time
			for(String indexName : indexNames)
			{
				Index<Node> index = graphDb.index().forNodes(indexName);
				IndexHits<Node> allItems = index.query("id", "*");
				while(allItems.hasNext())
				{
					Node item = allItems.next();
					int nonuser_views = 0;
					int user_views = 0;
					for(Relationship viewedByRel : item.getRelationships(DynamicRelationshipType.withName("Viewed_By")))
						if(Integer.parseInt(viewedByRel.getProperty("time").toString())>t)
							user_views++;
					if(item.hasProperty("latest_views"))
						nonuser_views = item.getProperty("latest_views").toString().split(",").length;
					item.setProperty("views", user_views*5 + nonuser_views);
				}
			}

			tx.success();
		}
		catch(Exception e){
			System.out.println("Exception @ calc_views()");
			System.out.println("Failed to calculate total views for articles / events / petitions from calc_views():" + e.getMessage());}
		finally{}
	}
	
	//update trending & featured tiles
	public void update_tiles_temp()
	{
		try (Transaction tx = graphDb.beginTx())
		{
			Index<Node> tilesIndex = graphDb.index().forNodes("tiles");
			Index<Node> articleIndex = graphDb.index().forNodes("article");
			Index<Node> eventIndex = graphDb.index().forNodes("event");
			Index<Node> petitionIndex = graphDb.index().forNodes("petition");
			Index<Node> townhallIndex = graphDb.index().forNodes("townhall");
			Index<Node> debateIndex = graphDb.index().forNodes("debate");
			
			int curTime = (int)(System.currentTimeMillis()/1000);
			int four_hours_ago = curTime - (3600*4); // 4 hours ago time
			int thirty_days_ago = curTime - (3600*24*30);

			ArrayList<ItemIdWeight> trendingTilesWithWeights = new ArrayList<ItemIdWeight>();
			ArrayList<ItemIdWeight> featuredTilesWithCreatedTime = new ArrayList<ItemIdWeight>();
			ArrayList<ItemIdWeight> articleTilesWithCreatedTime = new ArrayList<ItemIdWeight>();
			ArrayList<ItemIdWeight> eventTilesWithCreatedTime = new ArrayList<ItemIdWeight>();
			ArrayList<ItemIdWeight> petitionTilesWithCreatedTime = new ArrayList<ItemIdWeight>();
			ArrayList<ItemIdWeight> townhallTilesWithCreatedTime = new ArrayList<ItemIdWeight>();
			ArrayList<ItemIdWeight> debateTilesWithCreatedTime = new ArrayList<ItemIdWeight>();

			IndexHits<Node> articles = articleIndex.query("id", "*");
			IndexHits<Node> events = eventIndex.query("id", "*");
			IndexHits<Node> petitions = petitionIndex.query("id", "*");
			IndexHits<Node> townhalls = townhallIndex.query("id", "*");
			IndexHits<Node> debates = debateIndex.query("id", "*");
			
			while(articles.hasNext())
			{
				Node x = articles.next();

				if(x != null && Integer.parseInt(x.getProperty("space").toString()) == 0 && Integer.parseInt(x.getProperty("skip_from_special_tiles").toString()) == 0 && Integer.parseInt(x.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getOtherNode(x).getProperty("acc_type").toString()) != 3)
				{
					//add to trending list
					if(Integer.parseInt(x.getProperty("time_created").toString()) > thirty_days_ago && Integer.parseInt(x.getProperty("views").toString()) > 9 )
					{
						int views = Integer.parseInt(x.getProperty("views").toString());
						int in_weight = 0;
						for(Relationship eachRel: x.getRelationships(DynamicRelationshipType.withName("Comment_To_Article"),DynamicRelationshipType.withName("article_markfav"),DynamicRelationshipType.withName("article_voteup")))
							if(Integer.parseInt(x.getProperty("time").toString()) > four_hours_ago)
								in_weight = in_weight + Integer.parseInt(eachRel.getProperty("in_weight").toString());
						trendingTilesWithWeights.add(new ItemIdWeight(x.getProperty("article_id").toString(),(views+1+in_weight*20)/((((curTime-Integer.parseInt(x.getProperty("time_created").toString()))/3600)+1 )*90)));
					}
					//add to featured list
					if(Integer.parseInt(x.getProperty("approved").toString()) == 1)
						featuredTilesWithCreatedTime.add(new ItemIdWeight(x.getProperty("article_id").toString(),Integer.parseInt(x.getProperty("time_created").toString())));
					//add to others list
					articleTilesWithCreatedTime.add(new ItemIdWeight(x.getProperty("article_id").toString(),Integer.parseInt(x.getProperty("time_created").toString())));					
				}
			}
			
			while(events.hasNext())
			{
				Node x = events.next();

				if(x != null && Integer.parseInt(x.getProperty("space").toString()) == 0 && Integer.parseInt(x.getProperty("skip_from_special_tiles").toString()) == 0 && Integer.parseInt(x.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getOtherNode(x).getProperty("acc_type").toString()) != 3)
				{
					//add to trending list
					if(Integer.parseInt(x.getProperty("time_created").toString()) > thirty_days_ago && Integer.parseInt(x.getProperty("views").toString()) > 9 )
					{
						int views = Integer.parseInt(x.getProperty("views").toString());
						int in_weight = 0;
						for(Relationship eachRel: x.getRelationships(DynamicRelationshipType.withName("Comment_To_Event"),DynamicRelationshipType.withName("Is_Attending")))
							if(Integer.parseInt(x.getProperty("time").toString()) > four_hours_ago)
								in_weight = in_weight + Integer.parseInt(eachRel.getProperty("in_weight").toString());
						trendingTilesWithWeights.add(new ItemIdWeight(x.getProperty("event_id").toString(),(views+1+in_weight*30)/((((curTime-Integer.parseInt(x.getProperty("time_created").toString()))/3600)+1 )*90)));
					}
					//add to featured list
					if(Integer.parseInt(x.getProperty("approved").toString()) == 1)
						featuredTilesWithCreatedTime.add(new ItemIdWeight(x.getProperty("event_id").toString(),Integer.parseInt(x.getProperty("time_created").toString())));
					//add to others list
					eventTilesWithCreatedTime.add(new ItemIdWeight(x.getProperty("event_id").toString(),Integer.parseInt(x.getProperty("time_created").toString())));					
				}
			}

			while(petitions.hasNext())
			{
				Node x = petitions.next();

				if(x != null && Integer.parseInt(x.getProperty("space").toString()) == 0 && Integer.parseInt(x.getProperty("skip_from_special_tiles").toString()) == 0 && Integer.parseInt(x.getSingleRelationship(DynamicRelationshipType.withName("Petition_Written_By"),Direction.OUTGOING).getOtherNode(x).getProperty("acc_type").toString()) != 3)
				{
					//add to trending list
					if(Integer.parseInt(x.getProperty("time_created").toString()) > thirty_days_ago && Integer.parseInt(x.getProperty("views").toString()) > 9 )
					{
						int views = Integer.parseInt(x.getProperty("views").toString());
						int in_weight = 0;
						for(Relationship eachRel: x.getRelationships(DynamicRelationshipType.withName("Comment_To_Petition"),DynamicRelationshipType.withName("Signed_Petition")))
							if(Integer.parseInt(x.getProperty("time").toString()) > four_hours_ago)
								in_weight = in_weight + Integer.parseInt(eachRel.getProperty("in_weight").toString());
						trendingTilesWithWeights.add(new ItemIdWeight(x.getProperty("p_id").toString(),(views+1+in_weight*10)/((((curTime-Integer.parseInt(x.getProperty("time_created").toString()))/3600)+1 )*90)));
					}
					//add to featured list
					if(Integer.parseInt(x.getProperty("approved").toString()) == 1)
						featuredTilesWithCreatedTime.add(new ItemIdWeight(x.getProperty("p_id").toString(),Integer.parseInt(x.getProperty("time_created").toString())));
					//add to others list
					petitionTilesWithCreatedTime.add(new ItemIdWeight(x.getProperty("p_id").toString(),Integer.parseInt(x.getProperty("time_created").toString())));					
				}
			}

			while(townhalls.hasNext())
			{
				Node x = townhalls.next();

				if(x != null && Integer.parseInt(x.getProperty("space").toString()) == 0 && Integer.parseInt(x.getProperty("skip_from_special_tiles").toString()) == 0 && Integer.parseInt(x.getSingleRelationship(DynamicRelationshipType.withName("Townhall_Written_By"),Direction.OUTGOING).getOtherNode(x).getProperty("acc_type").toString()) != 3)
				{
					//add to trending list
					if(Integer.parseInt(x.getProperty("time_created").toString()) > thirty_days_ago && Integer.parseInt(x.getProperty("views").toString()) > 9 )
					{
						int views = Integer.parseInt(x.getProperty("views").toString());
						int in_weight = 0;
						for(Relationship eachRel: x.getRelationships(DynamicRelationshipType.withName("Asked_Question"),DynamicRelationshipType.withName("Voted_Townhall_Question"),DynamicRelationshipType.withName("Voted_Townhall_Answer"),DynamicRelationshipType.withName("Commented_On_Townhall")))
							if(Integer.parseInt(x.getProperty("time").toString()) > four_hours_ago)
								in_weight = in_weight + Integer.parseInt(eachRel.getProperty("in_weight").toString());
						trendingTilesWithWeights.add(new ItemIdWeight(x.getProperty("t_id").toString(),(views+1+in_weight*50)/((((curTime-Integer.parseInt(x.getProperty("time_created").toString()))/3600)+1 )*90)));
					}
					//add to featured list
					if(Integer.parseInt(x.getProperty("approved").toString()) == 1)
						featuredTilesWithCreatedTime.add(new ItemIdWeight(x.getProperty("t_id").toString(),Integer.parseInt(x.getProperty("time_created").toString())));
					//add to others list
					townhallTilesWithCreatedTime.add(new ItemIdWeight(x.getProperty("t_id").toString(),Integer.parseInt(x.getProperty("time_created").toString())));					
				}
			}

			while(debates.hasNext())
			{
				Node x = debates.next();

				if(x != null && Integer.parseInt(x.getProperty("space").toString()) == 0 && Integer.parseInt(x.getProperty("skip_from_special_tiles").toString()) == 0 && Integer.parseInt(x.getSingleRelationship(DynamicRelationshipType.withName("Debate_Written_By"),Direction.OUTGOING).getOtherNode(x).getProperty("acc_type").toString()) != 3)
				{
					//add to trending list
					if(Integer.parseInt(x.getProperty("time_created").toString()) > thirty_days_ago && Integer.parseInt(x.getProperty("views").toString()) > 9 )
					{
						int views = Integer.parseInt(x.getProperty("views").toString());
						int in_weight = 0;
						for(Relationship eachRel: x.getRelationships(DynamicRelationshipType.withName("Asked_Debate_Question"),DynamicRelationshipType.withName("Started_Debate_Argument"),DynamicRelationshipType.withName("Commented_On_Debate")))
							if(Integer.parseInt(x.getProperty("time").toString()) > four_hours_ago)
								in_weight = in_weight + Integer.parseInt(eachRel.getProperty("in_weight").toString());
						trendingTilesWithWeights.add(new ItemIdWeight(x.getProperty("d_id").toString(),(views+1+in_weight*50)/((((curTime-Integer.parseInt(x.getProperty("time_created").toString()))/3600)+1 )*90)));
					}
					//add to featured list
					if(Integer.parseInt(x.getProperty("approved").toString()) == 1)
						featuredTilesWithCreatedTime.add(new ItemIdWeight(x.getProperty("d_id").toString(),Integer.parseInt(x.getProperty("time_created").toString())));
					//add to others list
					debateTilesWithCreatedTime.add(new ItemIdWeight(x.getProperty("d_id").toString(),Integer.parseInt(x.getProperty("time_created").toString())));					
				}
			}

			StringBuffer trendingIdsList = new StringBuffer();
			StringBuffer featuredIdsList = new StringBuffer();
			StringBuffer articleIdsList = new StringBuffer();
			StringBuffer eventIdsList = new StringBuffer();
			StringBuffer petitionIdsList = new StringBuffer();
			StringBuffer townhallIdsList = new StringBuffer();
			StringBuffer debateIdsList = new StringBuffer();

			Collections.sort(trendingTilesWithWeights);
			for(ItemIdWeight eachItem : trendingTilesWithWeights)
				trendingIdsList.append(","+eachItem.id);
			trendingTilesWithWeights.clear();
			if(trendingIdsList.length()>0)
				trendingIdsList.replace(0, 1, "");
			Node trendingNode = tilesIndex.get("id", "trending").getSingle();
			trendingNode.setProperty("value", trendingIdsList);
			
			Collections.sort(featuredTilesWithCreatedTime);
			for(ItemIdWeight eachItem : featuredTilesWithCreatedTime)
				featuredIdsList.append(","+eachItem.id);
			featuredTilesWithCreatedTime.clear();
			if(featuredIdsList.length()>0)
				featuredIdsList.replace(0, 1, "");
			Node featuredNode = tilesIndex.get("id", "featured").getSingle();
			featuredNode.setProperty("value", featuredIdsList);
			Node latestNode = tilesIndex.get("id", "latest").getSingle();
			latestNode.setProperty("value", featuredIdsList);
			
			Collections.sort(articleTilesWithCreatedTime);
			for(ItemIdWeight eachItem : articleTilesWithCreatedTime)
				articleIdsList.append(","+eachItem.id);
			articleTilesWithCreatedTime.clear();
			if(articleIdsList.length()>0)
				articleIdsList.replace(0, 1, "");
			Node articleNode = tilesIndex.get("id", "article").getSingle();
			articleNode.setProperty("value", articleIdsList);
			
			Collections.sort(eventTilesWithCreatedTime);
			for(ItemIdWeight eachItem : eventTilesWithCreatedTime)
				eventIdsList.append(","+eachItem.id);
			eventTilesWithCreatedTime.clear();
			if(eventIdsList.length()>0)
				eventIdsList.replace(0, 1, "");
			Node eventNode = tilesIndex.get("id", "event").getSingle();
			eventNode.setProperty("value", eventIdsList);
			
			Collections.sort(petitionTilesWithCreatedTime);
			for(ItemIdWeight eachItem : petitionTilesWithCreatedTime)
				petitionIdsList.append(","+eachItem.id);
			petitionTilesWithCreatedTime.clear();
			if(petitionIdsList.length()>0)
				petitionIdsList.replace(0, 1, "");
			Node petitionNode = tilesIndex.get("id", "petition").getSingle();
			petitionNode.setProperty("value", petitionIdsList);
			
			Collections.sort(townhallTilesWithCreatedTime);
			for(ItemIdWeight eachItem : townhallTilesWithCreatedTime)
				townhallIdsList.append(","+eachItem.id);
			townhallTilesWithCreatedTime.clear();
			if(townhallIdsList.length()>0)
				townhallIdsList.replace(0, 1, "");
			Node townhallNode = tilesIndex.get("id", "townhall").getSingle();
			townhallNode.setProperty("value", townhallIdsList);
			

			Collections.sort(debateTilesWithCreatedTime);
			for(ItemIdWeight eachItem : debateTilesWithCreatedTime)
				debateIdsList.append(","+eachItem.id);
			debateTilesWithCreatedTime.clear();
			if(debateIdsList.length()>0)
				debateIdsList.replace(0, 1, "");
			Node debateNode = tilesIndex.get("id", "debate").getSingle();
			debateNode.setProperty("value", debateIdsList);
			
			tx.success();
		}
		catch(Exception e){
			System.out.println("Exception @ update_tiles_temp()");
			System.out.println("Failed to calculate trending/featured list from update_tiles_temp():" + e.getMessage());}
		finally{}	
	}
	
	public void update_tiles_with_stddev_zscore()
	{
		// no need to implement
	}
	
	public void update_tiles_td()
	{
		try (Transaction tx = graphDb.beginTx())
		{
			Index<Node> eventIndex = graphDb.index().forNodes("event");
			Index<Node> debateIndex = graphDb.index().forNodes("debate");
			Index<Node> townhallIndex = graphDb.index().forNodes("townhall");
			Index<Node> tileIndex = graphDb.index().forNodes("tiles");
			
			int curTime = (int)(System.currentTimeMillis()/1000);
			int t2 = curTime + 86400;
			
			IndexHits<Node> townhalls = townhallIndex.query("id","*");
			IndexHits<Node> debates = debateIndex.query("id","*");
			IndexHits<Node> events = eventIndex.query("id","*");
			
			StringBuffer allItems = new StringBuffer();

			for(Node townhall : townhalls)
				if(townhall != null && Integer.parseInt(townhall.getProperty("space").toString()) == 0 && Integer.parseInt(townhall.getProperty("skip_from_special_tiles").toString()) == 0 && Integer.parseInt(townhall.getSingleRelationship(DynamicRelationshipType.withName("Townhall_Written_By"),Direction.OUTGOING).getOtherNode(townhall).getProperty("acc_type").toString()) != 3 && Integer.parseInt(townhall.getProperty("t_date").toString())+3600 > curTime  && Integer.parseInt(townhall.getProperty("t_date").toString()) < t2 )
					allItems.append(townhall.getProperty("t_id").toString()+",");
			for(Node event : events)
				if(event != null && Integer.parseInt(event.getProperty("space").toString()) == 0 && Integer.parseInt(event.getProperty("skip_from_special_tiles").toString()) == 0 && Integer.parseInt(event.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getOtherNode(event).getProperty("acc_type").toString()) != 3 && Integer.parseInt(event.getProperty("event_date_time").toString()) > curTime  && Integer.parseInt(event.getProperty("event_date_time").toString()) < t2 )
					allItems.append(event.getProperty("event_id").toString()+",");
			for(Node debate : debates)
				if(debate != null && Integer.parseInt(debate.getProperty("space").toString()) == 0 && Integer.parseInt(debate.getProperty("skip_from_special_tiles").toString()) == 0 && Integer.parseInt(debate.getSingleRelationship(DynamicRelationshipType.withName("Debate_Written_By"),Direction.OUTGOING).getOtherNode(debate).getProperty("acc_type").toString()) != 3 && Integer.parseInt(debate.getProperty("d_date").toString())+3600 > curTime  && Integer.parseInt(debate.getProperty("d_date").toString()) < t2 )
					allItems.append(debate.getProperty("d_id").toString()+",");

			Node trending = tileIndex.get("id","trending").getSingle();
			
			String news = trending.getProperty("value").toString();
			for(String itemId : allItems.toString().split(","))
				news.replace(itemId, "");

			trending.setProperty("value", allItems.toString() + news);
			
			tx.success();
		}
		catch(Exception e){
			System.out.println("Exception @ update_tiles_td()");
			System.out.println("Failed to calculate upcoming events/townhalls/debates from update_tiles_td():" + e.getMessage());}
		finally{}	
	}
	
	public void avg_weights()
	{
		// no need to implement
	}
	
	public void negative_weights()
	{
		// no need to implement
	}
	
	public void calc_hash_trends()
	{
		// no need to implement
	}
	
	public void nouns_update()
	{
		// no useful , no need to implement
	}
	
	public void calc_user_tiles()
	{
		ArrayList<String> userNames = new ArrayList<String>();
		try (Transaction tx = graphDb.beginTx())
		{
			Index<Node> userIndex = graphDb.index().forNodes("user");
			IndexHits<Node> users = userIndex.query("id","*");
			while(users.hasNext())
				userNames.add(users.next().getProperty("user_name").toString());
			tx.success();
		}
		catch(Exception e){
			System.out.println("Exception @ calc_user_tiles()");
			System.out.println("Failed to calculate user personalized list for all users from calc_user_tiles():" + e.getMessage());}
		finally{}	
		
		for(String userName : userNames)
			calc_user_tiles(userName);
	}
	
	public void calc_user_tiles(String user_name)
	{
		try (Transaction tx = graphDb.beginTx())
		{
			int curTime = (int) (System.currentTimeMillis()/1000);
			int t1 = curTime - 86400;
			int t2 = curTime - 86400*3;
			
			Index<Node> userTilesIndex = graphDb.index().forNodes("user_tiles");
			Index<Node> userIndex = graphDb.index().forNodes("user");
			Index<Node> hashTagIndex = graphDb.index().forNodes("sub_category");
			
			Node user = userIndex.get("id", user_name).getSingle();
			if(user != null)
			{
				HashMap<Node, Integer> hashtagMapWithCount = new HashMap<Node, Integer>();
				for(Relationship rel1 : user.getRelationships(DynamicRelationshipType.withName("article_voteup"),DynamicRelationshipType.withName("article_markfav"),DynamicRelationshipType.withName("Is_Attending"),DynamicRelationshipType.withName("Signed_Petition")))
				{
					Node otherNode = rel1.getOtherNode(user);
					for(Relationship tempRel : otherNode.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Article"),DynamicRelationshipType.withName("Tag_Of_Article")))
					{
						Node hashNode = tempRel.getOtherNode(otherNode);
						if(hashtagMapWithCount.containsKey(hashNode))
							hashtagMapWithCount.put(hashNode, hashtagMapWithCount.get(hashNode)+2);
						else
							hashtagMapWithCount.put(hashNode, 2);
					}
				}
				for(Relationship rel2 : user.getRelationships(DynamicRelationshipType.withName("Viewed_By")))
				{
					Node otherNode = rel2.getOtherNode(user);
					for(Relationship tempRel : otherNode.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Article"),DynamicRelationshipType.withName("Tag_Of_Article"),DynamicRelationshipType.withName("Belongs_To_Subcategory_Event"),DynamicRelationshipType.withName("Tag_Of_Event"),DynamicRelationshipType.withName("Belongs_To_Subcategory_Petition"),DynamicRelationshipType.withName("Tag_Of_Petition")))
					{
						Node hashNode = tempRel.getOtherNode(otherNode);
						if(hashtagMapWithCount.containsKey(hashNode))
							hashtagMapWithCount.put(hashNode, hashtagMapWithCount.get(hashNode)+1);
						else
							hashtagMapWithCount.put(hashNode, 1);
					}
				}
				
				ArrayList<ItemIdWeight> list = new ArrayList<ItemIdWeight>();
				
				for(Node temp : hashtagMapWithCount.keySet())
					list.add(new ItemIdWeight(temp.getProperty("name").toString(), hashtagMapWithCount.get(temp)));
				hashtagMapWithCount.clear();
				Collections.sort(list);

				ArrayList<Node> persoTileNodes = new ArrayList<Node>();
				ArrayList<Node> friendsTileNodes = new ArrayList<Node>();
				
				int count = 0;
				for(ItemIdWeight item : list)
				{
					if(count >= 25) break;
					count++;
					
					Node hashNode = hashTagIndex.get("id", item.id).getSingle();
					
					for(Relationship rel1 : hashNode.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Article"),DynamicRelationshipType.withName("Tag_Of_Article")))
					{
						Node artNode = rel1.getOtherNode(hashNode);
						if(Integer.parseInt(artNode.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getOtherNode(artNode).getProperty("acc_type").toString()) != 3 && Integer.parseInt(artNode.getProperty("space").toString())==0 && Integer.parseInt(artNode.getProperty("skip_from_special_tiles").toString())==0 && !persoTileNodes.contains(artNode))
							persoTileNodes.add(artNode);
					}

					for(Relationship rel1 : hashNode.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Event"),DynamicRelationshipType.withName("Tag_Of_Event")))
					{
						Node evtNode = rel1.getOtherNode(hashNode);
						if(Integer.parseInt(evtNode.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getOtherNode(evtNode).getProperty("acc_type").toString()) != 3 && Integer.parseInt(evtNode.getProperty("space").toString())==0 && Integer.parseInt(evtNode.getProperty("skip_from_special_tiles").toString())==0 &&  !persoTileNodes.contains(evtNode))
							persoTileNodes.add(evtNode);
					}

					for(Relationship rel1 : hashNode.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Petition"),DynamicRelationshipType.withName("Tag_Of_Petition")))
					{
						Node petNode = rel1.getOtherNode(hashNode);
						if(Integer.parseInt(petNode.getSingleRelationship(DynamicRelationshipType.withName("Petition_Written_By"),Direction.OUTGOING).getOtherNode(petNode).getProperty("acc_type").toString()) != 3 && Integer.parseInt(petNode.getProperty("space").toString())==0 && Integer.parseInt(petNode.getProperty("skip_from_special_tiles").toString())==0 &&  !persoTileNodes.contains(petNode))
							persoTileNodes.add(petNode);
					}
					
				}
				
				for(Relationship rel : user.getRelationships(DynamicRelationshipType.withName("Follows")))
				{
					Node followingUser = rel.getOtherNode(user);
					if(Integer.parseInt(followingUser.getProperty("acc_type").toString())==3)
						continue;
					for(Relationship eachItemCreatedByFollowingUser : followingUser.getRelationships(DynamicRelationshipType.withName("Article_Written_By"),DynamicRelationshipType.withName("Event_Created_By"),DynamicRelationshipType.withName("Petition_Written_By"),DynamicRelationshipType.withName("Townhall_Written_By"),DynamicRelationshipType.withName("Debate_Written_By")))
					{
						Node itemNode = eachItemCreatedByFollowingUser.getOtherNode(followingUser);
						if( Integer.parseInt(itemNode.getProperty("space").toString())==0 && Integer.parseInt(itemNode.getProperty("skip_from_special_tiles").toString())==0 && !friendsTileNodes.contains(itemNode))
							friendsTileNodes.add(itemNode);
					}
				}

				ArrayList<Node> one = new ArrayList<Node>();
				ArrayList<Node> two = new ArrayList<Node>();
				ArrayList<Node> three = new ArrayList<Node>();
				
				for(Node item : friendsTileNodes)
				{
					if(persoTileNodes.contains(item))
					{
						if(Integer.parseInt(item.getProperty("time_created").toString())>t1)
							one.add(item);
						else if(Integer.parseInt(item.getProperty("time_created").toString())>t2)
							two.add(item);
						else three.add(item);
					}
					else three.add(item);
				}
				for(Node item: persoTileNodes)
					if(!one.contains(item) && !two.contains(item) && !three.contains(item))
						three.add(item);
				
				Collections.sort(one, TimeCreatedComparatorForNodes);
				Collections.sort(two, TimeCreatedComparatorForNodes);
				Collections.sort(three, TimeCreatedComparatorForNodes);
				
				StringBuffer persoTiles = new StringBuffer();
				for(Node item : one)
				{
					if(item.getProperty("__CLASS__").toString().equals("Saddahaq.article"))
						persoTiles.append(","+item.getProperty("article_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.event"))
						persoTiles.append(","+item.getProperty("event_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.petition"))
						persoTiles.append(","+item.getProperty("p_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.townhall"))
							persoTiles.append(","+item.getProperty("t_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.debate"))
						persoTiles.append(","+item.getProperty("d_id").toString());
				}
				for(Node item : two)
				{
					if(item.getProperty("__CLASS__").toString().equals("Saddahaq.article"))
						persoTiles.append(","+item.getProperty("article_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.event"))
						persoTiles.append(","+item.getProperty("event_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.petition"))
						persoTiles.append(","+item.getProperty("p_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.townhall"))
							persoTiles.append(","+item.getProperty("t_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.debate"))
						persoTiles.append(","+item.getProperty("d_id").toString());
				}
				for(Node item : three)
				{
					if(item.getProperty("__CLASS__").toString().equals("Saddahaq.article"))
						persoTiles.append(","+item.getProperty("article_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.event"))
						persoTiles.append(","+item.getProperty("event_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.petition"))
						persoTiles.append(","+item.getProperty("p_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.townhall"))
							persoTiles.append(","+item.getProperty("t_id").toString());
					else if(item.getProperty("__CLASS__").toString().equals("Saddahaq.debate"))
						persoTiles.append(","+item.getProperty("d_id").toString());
				}
				
				if(persoTiles.length() > 1)
					persoTiles.replace(0, 1, "");
				Node user_tiles_node = userTilesIndex.get("id",user.getProperty("user_name").toString()).getSingle();
				user_tiles_node.setProperty("news_Personalized", persoTiles.toString());
			}
			
			tx.success();
		}
		catch(Exception e){
			System.out.println("Exception @ calc_user_tiles(String user_name)");
			System.out.println("Failed to calculate user personalized list for given user:"+user_name+" from calc_user_tiles("+user_name+"):" + e.getMessage());}
		finally{}	
	}
	
	public void calc_local_tiles()
	{
		try (Transaction tx = graphDb.beginTx())
		{
			int curTime = (int) (System.currentTimeMillis()/1000);
			int t1 = curTime - 86400*2;
			
			Index<Node> locationIndex = graphDb.index().forNodes("location");
			
			IndexHits<Node> locationNodes = locationIndex.query("id","*");
			while(locationNodes.hasNext())
			{
				ArrayList<Node> articles = new ArrayList<Node>();
				Node locationNode = locationNodes.next();
				for(Relationship each : locationNode.getRelationships(DynamicRelationshipType.withName("Belongs_To_Location_Article")))
				{
					Node artNode = each.getOtherNode(locationNode);
					if(Integer.parseInt(artNode.getProperty("time_created").toString()) > t1 && Integer.parseInt(artNode.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getOtherNode(artNode).getProperty("acc_type").toString()) != 3 && Integer.parseInt(artNode.getProperty("space").toString())==0 && Integer.parseInt(artNode.getProperty("skip_from_special_tiles").toString())==0)
						articles.add(artNode);
				}
				Collections.sort(articles, TimeCreatedComparatorForNodes);
				StringBuffer list = new StringBuffer();
				for(Node art : articles)
					list.append(","+art.getProperty("article_id").toString());
				if(list.length()>1)
					list.replace(0, 1, "");
				locationNode.setProperty("tiles", list.toString());
			}
			
			tx.success();
		}
		catch(Exception e){
			System.out.println("Exception @ calc_local_tiles()");
			System.out.println("Failed to calculate location based list for each location from calc_local_tiles():" + e.getMessage());}
		finally{}
	}

	public void sample_test()
	{
		System.out.println("Calling update (old calculation) : Update will run after 15 min, then every 15 min");
        Timer timer = new Timer ();
		TimerTask hourlyTask = new TimerTask () {
		    @Override
		    public void run () {		      
		    	System.out.println("Running update");
		    	try{
		    		calc_views();
		    		update_tiles_temp(); //trending and featured - @author Yash
		    		//update_tiles_with_stddev_zscore(); //trending and featured - @author Kalyan
		    		update_tiles_td();	//update trending based on last three days
		    		//avg_weights();
		    		//negative_weights();
		    		//calc_hash_trends();
		    		nouns_update();
		    		calc_user_tiles();
		    		calc_local_tiles();
		    	}
		    	catch(Exception ex){
                  System.out.println("Something went wrong, while running update :"+ex.getMessage());
                  ex.printStackTrace();
		    	}
		  }
		};
		// schedule the task to run for every 15 min 1000*900
		timer.schedule (hourlyTask, 1000*900, 1000*900);
	}
	
	public void main()
	{
		//starting thrift server
		
		calc_views();
		update_tiles_temp(); //trending and featured - @author Yash
		//update_tiles_with_stddev_zscore(); //trending and featured - @author Kalyan
		update_tiles_td();	//update trending based on last three days
		//avg_weights();
		//negative_weights();
		//calc_hash_trends();
		nouns_update();
		calc_user_tiles();
		calc_local_tiles();
		
		insert_dummy_data();

		try{
			
			/*TServerSocket serverTransport = new TServerSocket(9779);
			User_nodeService.Processor<User_nodeService.Iface> processor = new User_nodeService.Processor<User_nodeService.Iface>(this);
			Args serverArgs = new Args(serverTransport);
			serverArgs.processor(processor);
			TServer server = new TThreadPoolServer(serverArgs);
			System.out.println("Sadda haq ethe rakh");
			server.serve();*/
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void insert_dummy_data()
	{
		try{
			System.out.println(get_tiles("","",5,0,"","t"));
			System.out.println(get_tiles("","",5,0,"","f"));
			int t = (int)(System.currentTimeMillis()/1000);
			System.out.println(create_user("user99", "user99", "user99", "user99","user99" ,1,t,100,"2"));
			System.out.println(create_article("user99", "new_article_2.2.0", "new_title_id_2.2.0", "new title 2.2.0" , " new content for new article 2.2.0", " new summery for new article 2.2.0", "new fut image", "", "hash1new", "hash2new", "", t, "", "", "", 0, 0, 0, "en"));
			System.out.println(get_tiles("","",5,0,"","t"));
			System.out.println(get_tiles("","",5,0,"","f"));
			System.out.println(featured_item("a", "new_article_2.2.0"));
			System.out.println(get_tiles("","",5,0,"","f"));
			System.out.println(featured_item("a", "new_article_2.2.0"));
			System.out.println(get_tiles("","",5,0,"","f"));
			System.out.println(delete_article("new_article_2.2.0"));
			System.out.println(get_tiles("","",5,0,"","t"));
			System.out.println(get_tiles("","",5,0,"","f"));
			
		}
		catch(Exception e)
		{
			System.out.println("Exception : " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	private JSONObject getArticleJSONForTile(Node article, boolean isPinned, Node userNode)
	{
		JSONObject json = new JSONObject();
		
		int number_of_votes_lessTwo = 0;
		
		Node author = article.getSingleRelationship(DynamicRelationshipType.withName("Article_Written_By"),Direction.OUTGOING).getEndNode();
		
		//prepare the list of commented users (unique)
		ArrayList<Node> commented_usersList = new ArrayList<Node>();
		ArrayList<Relationship> commentRelationsList = new ArrayList<Relationship>();
		for(Relationship eachCommentRel: article.getRelationships(DynamicRelationshipType.withName("Comment_To_Article")))
			commentRelationsList.add(eachCommentRel);
		Collections.sort(commentRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachCommentRel: commentRelationsList)
			if(!commented_usersList.contains(eachCommentRel.getOtherNode(article).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"), Direction.OUTGOING).getEndNode()))
				commented_usersList.add(eachCommentRel.getOtherNode(article).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"), Direction.OUTGOING).getEndNode());
		
		//preparing json array for commented users
		JSONArray commentedUsersJSONArray = new JSONArray();
		if(commented_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("FN", commented_usersList.get(0).getProperty("first_name").toString()+" "+commented_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UN", commented_usersList.get(0).getProperty("user_name").toString());
			commentedUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("FN", commented_usersList.get(1).getProperty("first_name").toString()+" "+commented_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UN", commented_usersList.get(1).getProperty("user_name").toString());
			commentedUsersJSONArray.put(userJSON2);
		}
		else{
			for(Node votedUser: commented_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("Name", votedUser.getProperty("first_name").toString()+" "+votedUser.getProperty("last_name").toString());
				userJSON.put("UName", votedUser.getProperty("user_name").toString());
				commentedUsersJSONArray.put(userJSON);
			}
		}
		
		//prepare the list of voted users
		ArrayList<Node> voted_usersList = new ArrayList<Node>();
		ArrayList<Relationship> voteupRelationsList = new ArrayList<Relationship>();
		for(Relationship eachVoteupRel: article.getRelationships(DynamicRelationshipType.withName("article_voteup")))
			voteupRelationsList.add(eachVoteupRel);
		Collections.sort(voteupRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachVotesRel: voteupRelationsList)
			if(!voted_usersList.contains(eachVotesRel.getOtherNode(article)))
				voted_usersList.add(eachVotesRel.getOtherNode(article));
		
		//preparing json array for votedup users
		JSONArray votedUsersJSONArray = new JSONArray();
		if(voted_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("Name", voted_usersList.get(0).getProperty("first_name").toString()+" "+voted_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UName", voted_usersList.get(0).getProperty("user_name").toString());
			votedUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("Name", voted_usersList.get(1).getProperty("first_name").toString()+" "+voted_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UName", voted_usersList.get(1).getProperty("user_name").toString());
			votedUsersJSONArray.put(userJSON2);
			number_of_votes_lessTwo = voted_usersList.size() - 2;
		}
		else{
			for(Node votedUser: voted_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("Name", votedUser.getProperty("first_name").toString()+" "+votedUser.getProperty("last_name").toString());
				userJSON.put("UName", votedUser.getProperty("user_name").toString());
				votedUsersJSONArray.put(userJSON);
			}
			number_of_votes_lessTwo = voted_usersList.size();
		}
		
		//get list of spaces tagged to this article
		ArrayList<Node> taggedSpaces = new ArrayList<Node>();
		StringBuffer space_title_list = new StringBuffer();
		StringBuffer space_title_id_list = new StringBuffer();
		for(Relationship space_tagged_rel: article.getRelationships(DynamicRelationshipType.withName("Article_Tagged_To_Space")))
		{
			taggedSpaces.add(space_tagged_rel.getOtherNode(article));
			space_title_list.append(space_tagged_rel.getOtherNode(article).getProperty("space_title").toString()+",");
			space_title_id_list.append(space_tagged_rel.getOtherNode(article).getProperty("space_title_id").toString()+",");
		}
		
		boolean isMarkedReadLater = false;
		if(userNode != null)
			for(Relationship readLaterRel: article.getRelationships(DynamicRelationshipType.withName("article_readlater")))
				if(userNode.equals(readLaterRel.getOtherNode(article)))
				{
					isMarkedReadLater = true;
					break;
				}
		
/*		String category = "";
		for(Relationship belongs_to_category_relation: article.getRelationships(DynamicRelationshipType.withName("Belongs_To_Category")))
			if(!"all".equalsIgnoreCase(belongs_to_category_relation.getOtherNode(article).getProperty("name").toString()))
			{
				category = belongs_to_category_relation.getOtherNode(article).getProperty("name").toString();
				break;
			}*/
		
		String sub_category = "";  //nothing but hashtag
		for(Relationship belongs_to_subcategory_relation: article.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Article")))
			if(belongs_to_subcategory_relation.getOtherNode(article).hasProperty("main"))
			{
				sub_category = belongs_to_subcategory_relation.getOtherNode(article).getProperty("name").toString();
				break;
			}
		
		json.put("P_CELEBRITY_UNAME", "");
		json.put("P_MODERATOR_UNAME", "");
		json.put("P_MODERATOR_FULLNAME", "");
		json.put("UST", author.getProperty("acc_type").toString());
		json.put("P_Language", article.getProperty("lang").toString());
		json.put("P_EventEndTime", "");
		json.put("Comment_Count_Unique", commented_usersList.size());
		json.put("ev", 0);
		json.put("v_users", votedUsersJSONArray);
		json.put("votes", number_of_votes_lessTwo);
		json.put("Commented_Users", commentedUsersJSONArray);
		json.put("Comment_Count", commentRelationsList.size());
		json.put("Is_Neo4j", true);
		json.put("P_Pin", isPinned);
		json.put("P_Id", article.getProperty("article_id").toString());
		json.put("P_Author", author.getProperty("user_name").toString());
		json.put("P_Author_FullName", author.getProperty("first_name").toString()+" "+author.getProperty("last_name").toString());
		json.put("P_IsMarkedReadLater", isMarkedReadLater);
		json.put("P_Title", article.getProperty("article_title").toString());
		json.put("P_Title_ID", article.getProperty("article_title_id").toString());
//		json.put("P_Category", category);
		json.put("P_SubCategory", sub_category);
		json.put("P_Num_Comments", commentRelationsList.size());
		json.put("P_Feature_Image", article.getProperty("article_featured_img").toString());
		json.put("P_Smry", article.getProperty("article_summary").toString());
		json.put("P_TimeCreated", article.getProperty("time_created").toString());
		json.put("P_EventLocation", "");
		json.put("P_EventStartTime", "");
		json.put("P_EventAttendStatus", false);
		json.put("P_SignsRequired", "");
		json.put("P_PetitionSignStatus", false);
		json.put("Space_Title", space_title_list.toString());
		json.put("Space_TitleId", space_title_id_list.toString());
		
		return json;
	}
	
	private JSONObject getEventJSONForTile(Node event, boolean isPinned, Node userNode)
	{
		JSONObject json = new JSONObject();
		
		int number_of_attendings_lessTwo = 0;
		
		Node author = event.getSingleRelationship(DynamicRelationshipType.withName("Event_Created_By"),Direction.OUTGOING).getEndNode();
		
		//prepare the list of commented users (unique)
		ArrayList<Node> commented_usersList = new ArrayList<Node>();
		ArrayList<Relationship> commentRelationsList = new ArrayList<Relationship>();
		for(Relationship eachCommentRel: event.getRelationships(DynamicRelationshipType.withName("Comment_To_Event")))
			commentRelationsList.add(eachCommentRel);
		Collections.sort(commentRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachCommentRel: commentRelationsList)
			if(!commented_usersList.contains(eachCommentRel.getOtherNode(event).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"), Direction.OUTGOING).getEndNode()))
				commented_usersList.add(eachCommentRel.getOtherNode(event).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"), Direction.OUTGOING).getEndNode());
		
		//preparing json array for commented users
		JSONArray commentedUsersJSONArray = new JSONArray();
		if(commented_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("FN", commented_usersList.get(0).getProperty("first_name").toString()+" "+commented_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UN", commented_usersList.get(0).getProperty("user_name").toString());
			commentedUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("FN", commented_usersList.get(1).getProperty("first_name").toString()+" "+commented_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UN", commented_usersList.get(1).getProperty("user_name").toString());
			commentedUsersJSONArray.put(userJSON2);
		}
		else{
			for(Node votedUser: commented_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("FN", votedUser.getProperty("first_name").toString()+" "+votedUser.getProperty("last_name").toString());
				userJSON.put("UN", votedUser.getProperty("user_name").toString());
				commentedUsersJSONArray.put(userJSON);
			}
		}

		//prepare the list of attending users
		ArrayList<Node> attending_usersList = new ArrayList<Node>();
		ArrayList<Relationship> attendingRelationsList = new ArrayList<Relationship>();
		for(Relationship eachAttendingRel: event.getRelationships(DynamicRelationshipType.withName("Is_Attending")))
			attendingRelationsList.add(eachAttendingRel);
		Collections.sort(attendingRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachAttendingRel: attendingRelationsList)
			if(!attending_usersList.contains(eachAttendingRel.getOtherNode(event)))
				attending_usersList.add(eachAttendingRel.getOtherNode(event));
		
		//preparing json array for attending users
		JSONArray attendingUsersJSONArray = new JSONArray();
		if(attending_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("Name", attending_usersList.get(0).getProperty("first_name").toString()+" "+attending_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UName", attending_usersList.get(0).getProperty("user_name").toString());
			attendingUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("Name", attending_usersList.get(1).getProperty("first_name").toString()+" "+attending_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UName", attending_usersList.get(1).getProperty("user_name").toString());
			attendingUsersJSONArray.put(userJSON2);
			number_of_attendings_lessTwo = attending_usersList.size() - 2;
		}
		else{
			for(Node votedUser: attending_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("Name", votedUser.getProperty("first_name").toString()+" "+votedUser.getProperty("last_name").toString());
				userJSON.put("UName", votedUser.getProperty("user_name").toString());
				attendingUsersJSONArray.put(userJSON);
			}
			number_of_attendings_lessTwo = attending_usersList.size();
		}
		
		boolean isAttending = false;
		if(userNode != null)
			for(Node votedUser: attending_usersList)
				if(userNode.equals(votedUser))
				{
					isAttending = true;
					break;
				}
		
		boolean isMarkedReadLater = false;
		if(userNode != null)
			for(Relationship readLaterRel: event.getRelationships(DynamicRelationshipType.withName("event_readlater")))
				if(userNode.equals(readLaterRel.getOtherNode(event)))
				{
					isMarkedReadLater = true;
					break;
				}
		
/*		String category = "";
		for(Relationship belongs_to_category_relation: event.getRelationships(DynamicRelationshipType.withName("Belongs_To_Event_Category")))
			if(!"all".equalsIgnoreCase(belongs_to_category_relation.getOtherNode(event).getProperty("name").toString()))
			{
				category = belongs_to_category_relation.getOtherNode(event).getProperty("name").toString();
				break;
			}*/
		
		String sub_category = ""; //nothing but hashtag
		for(Relationship belongs_to_subcategory_relation: event.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Event")))
			if(belongs_to_subcategory_relation.getOtherNode(event).hasProperty("main"))
			{
				sub_category = belongs_to_subcategory_relation.getOtherNode(event).getProperty("name").toString();
				break;
			}
		
		//get list of spaces tagged to this article
		ArrayList<Node> taggedSpaces = new ArrayList<Node>();
		StringBuffer space_title_list = new StringBuffer();
		StringBuffer space_title_id_list = new StringBuffer();
		for(Relationship space_tagged_rel: event.getRelationships(DynamicRelationshipType.withName("Event_Tagged_To_Space")))
		{
			taggedSpaces.add(space_tagged_rel.getOtherNode(event));
			space_title_list.append(space_tagged_rel.getOtherNode(event).getProperty("space_title").toString()+",");
			space_title_id_list.append(space_tagged_rel.getOtherNode(event).getProperty("space_title_id").toString()+",");
		}
		
		json.put("P_CELEBRITY_UNAME", "");
		json.put("P_MODERATOR_UNAME", "");
		json.put("P_MODERATOR_FULLNAME", "");
		json.put("UST", author.getProperty("acc_type").toString());
		json.put("P_Language", event.getProperty("lang").toString());
		json.put("P_EventEndTime", event.getProperty("event_date_time_closing").toString());
		json.put("Comment_Count_Unique", commented_usersList.size());
		json.put("ev", 1);
		json.put("v_users", attendingUsersJSONArray);
		json.put("votes", number_of_attendings_lessTwo);
		json.put("Commented_Users", commentedUsersJSONArray);
		json.put("Comment_Count", commentRelationsList.size());
		json.put("Is_Neo4j", true);
		json.put("P_Pin", isPinned);
		json.put("P_Id", event.getProperty("event_id").toString());
		json.put("P_Author", author.getProperty("user_name").toString());
		json.put("P_Author_FullName", author.getProperty("first_name").toString()+" "+author.getProperty("last_name").toString());
		json.put("P_IsMarkedReadLater", isMarkedReadLater);
		json.put("P_Title", event.getProperty("event_title").toString());
		json.put("P_Title_ID", event.getProperty("event_title_id").toString());
//		json.put("P_Category", category);
		json.put("P_SubCategory", sub_category);
		json.put("P_Num_Comments", commentRelationsList.size());
		json.put("P_Feature_Image", event.getProperty("event_featured_img").toString());
		json.put("P_Smry", event.getProperty("event_summary").toString());
		json.put("P_TimeCreated", event.getProperty("time_created").toString());
		json.put("P_EventLocation", event.getProperty("event_location").toString());
		json.put("P_EventStartTime", event.getProperty("event_date_time").toString());
		json.put("P_EventAttendStatus", isAttending);
		json.put("P_SignsRequired", "");
		json.put("P_PetitionSignStatus", false);
		json.put("Space_Title", space_title_list.toString());
		json.put("Space_TitleId", space_title_id_list.toString());
		
		return json;
	}
	
	private JSONObject getPetitionJSONForTile(Node petition, boolean isPinned, Node userNode)
	{
		JSONObject json = new JSONObject();
		
		int number_of_signs_lessTwo = 0;
		
		Node author = petition.getSingleRelationship(DynamicRelationshipType.withName("Petition_Written_By"),Direction.OUTGOING).getEndNode();
		
		//prepare the list of commented users (unique)
		ArrayList<Node> commented_usersList = new ArrayList<Node>();
		ArrayList<Relationship> commentRelationsList = new ArrayList<Relationship>();
		for(Relationship eachCommentRel: petition.getRelationships(DynamicRelationshipType.withName("Comment_To_Petition")))
			commentRelationsList.add(eachCommentRel);
		Collections.sort(commentRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachCommentRel: commentRelationsList)
			if(!commented_usersList.contains(eachCommentRel.getOtherNode(petition).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"), Direction.OUTGOING).getEndNode()))
				commented_usersList.add(eachCommentRel.getOtherNode(petition).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"), Direction.OUTGOING).getEndNode());
		
		//preparing json array for commented users
		JSONArray commentedUsersJSONArray = new JSONArray();
		if(commented_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("FN", commented_usersList.get(0).getProperty("first_name").toString()+" "+commented_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UN", commented_usersList.get(0).getProperty("user_name").toString());
			commentedUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("FN", commented_usersList.get(1).getProperty("first_name").toString()+" "+commented_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UN", commented_usersList.get(1).getProperty("user_name").toString());
			commentedUsersJSONArray.put(userJSON2);
		}
		else{
			for(Node votedUser: commented_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("FN", votedUser.getProperty("first_name").toString()+" "+votedUser.getProperty("last_name").toString());
				userJSON.put("UN", votedUser.getProperty("user_name").toString());
				commentedUsersJSONArray.put(userJSON);
			}
		}

		//prepare the list of signed users
		ArrayList<Node> signed_usersList = new ArrayList<Node>();
		ArrayList<Relationship> signedRelationsList = new ArrayList<Relationship>();
		for(Relationship eachVoteupRel: petition.getRelationships(DynamicRelationshipType.withName("Signed_Petition")))
			signedRelationsList.add(eachVoteupRel);
		Collections.sort(signedRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachSignRel: signedRelationsList)
			if(!signed_usersList.contains(eachSignRel.getOtherNode(petition)))
				signed_usersList.add(eachSignRel.getOtherNode(petition));
		
		//preparing json array for attending users
		JSONArray signedUsersJSONArray = new JSONArray();
		if(signed_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("Name", signed_usersList.get(0).getProperty("first_name").toString()+" "+signed_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UName", signed_usersList.get(0).getProperty("user_name").toString());
			signedUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("Name", signed_usersList.get(1).getProperty("first_name").toString()+" "+signed_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UName", signed_usersList.get(1).getProperty("user_name").toString());
			signedUsersJSONArray.put(userJSON2);
			number_of_signs_lessTwo = signed_usersList.size() - 2;
		}
		else{
			for(Node signedUser: signed_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("Name", signedUser.getProperty("first_name").toString()+" "+signedUser.getProperty("last_name").toString());
				userJSON.put("UName", signedUser.getProperty("user_name").toString());
				signedUsersJSONArray.put(userJSON);
			}
			number_of_signs_lessTwo = signed_usersList.size();
		}
		
		boolean isSigned = false;
		if(userNode != null)
			for(Node votedUser: signed_usersList)
				if(userNode.equals(votedUser))
				{
					isSigned = true;
					break;
				}
		
		boolean isMarkedReadLater = false;
		if(userNode != null)
			for(Relationship readLaterRel: petition.getRelationships(DynamicRelationshipType.withName("petition_readlater")))
				if(userNode.equals(readLaterRel.getOtherNode(petition)))
				{
					isMarkedReadLater = true;
					break;
				}
		
/*		String category = "";
		for(Relationship belongs_to_category_relation: petition.getRelationships(DynamicRelationshipType.withName("Belongs_To_Petition_Category")))
			if(!"all".equalsIgnoreCase(belongs_to_category_relation.getOtherNode(petition).getProperty("name").toString()))
			{
				category = belongs_to_category_relation.getOtherNode(petition).getProperty("name").toString();
				break;
			}*/
		
		String sub_category = ""; //nothing but hashtag
		for(Relationship belongs_to_subcategory_relation: petition.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Petition")))
			if(belongs_to_subcategory_relation.getOtherNode(petition).hasProperty("main"))
			{
				sub_category = belongs_to_subcategory_relation.getOtherNode(petition).getProperty("name").toString();
				break;
			}
		
		//get list of spaces tagged to this article
		ArrayList<Node> taggedSpaces = new ArrayList<Node>();
		StringBuffer space_title_list = new StringBuffer();
		StringBuffer space_title_id_list = new StringBuffer();
		for(Relationship space_tagged_rel: petition.getRelationships(DynamicRelationshipType.withName("Petition_Tagged_To_Space")))
		{
			taggedSpaces.add(space_tagged_rel.getOtherNode(petition));
			space_title_list.append(space_tagged_rel.getOtherNode(petition).getProperty("space_title").toString()+",");
			space_title_id_list.append(space_tagged_rel.getOtherNode(petition).getProperty("space_title_id").toString()+",");
		}
	
		json.put("P_CELEBRITY_UNAME", "");
		json.put("P_MODERATOR_UNAME", "");
		json.put("P_MODERATOR_FULLNAME", "");
		json.put("UST", author.getProperty("acc_type").toString());
		json.put("P_Language", petition.getProperty("lang").toString());
		json.put("P_EventEndTime", "");
		json.put("Comment_Count_Unique", commented_usersList.size());
		json.put("ev", 2);
		json.put("v_users", signedUsersJSONArray);
		json.put("votes", number_of_signs_lessTwo);
		json.put("Commented_Users", commentedUsersJSONArray);
		json.put("Comment_Count", commentRelationsList.size());
		json.put("Is_Neo4j", true);
		json.put("P_Pin", isPinned);
		json.put("P_Id", petition.getProperty("p_id").toString());
		json.put("P_Author", author.getProperty("user_name").toString());
		json.put("P_Author_FullName", author.getProperty("first_name").toString()+" "+author.getProperty("last_name").toString());
		json.put("P_IsMarkedReadLater", isMarkedReadLater);
		json.put("P_Title", petition.getProperty("p_title").toString());
		json.put("P_Title_ID", petition.getProperty("p_title_id").toString());
//		json.put("P_Category", category);
		json.put("P_SubCategory", sub_category);
		json.put("P_Num_Comments", commentRelationsList.size());
		json.put("P_Feature_Image", petition.getProperty("p_img_url").toString());
		json.put("P_Smry", petition.getProperty("p_content").toString());
		json.put("P_TimeCreated", petition.getProperty("time_created").toString());
		json.put("P_EventLocation", "");
		json.put("P_EventStartTime", "");
		json.put("P_EventAttendStatus", false);
		json.put("P_SignsRequired", Integer.parseInt(petition.getProperty("p_target").toString())-Integer.parseInt(petition.getProperty("p_count").toString()));
		json.put("P_PetitionSignStatus", isSigned);
		json.put("Space_Title", space_title_list.toString());
		json.put("Space_TitleId", space_title_id_list.toString());
		
		return json;
	}
	
	private JSONObject getDebateJSONForTile(Node debate, boolean isPinned, Node userNode)
	{
		JSONObject json = new JSONObject();
		
		int number_of_asks_lessTwo = 0;
		
		Node author = debate.getSingleRelationship(DynamicRelationshipType.withName("Debate_Written_By"),Direction.OUTGOING).getEndNode();
		
		//prepare the list of commented users (unique)
		ArrayList<Node> commented_usersList = new ArrayList<Node>();
		ArrayList<Relationship> commentRelationsList = new ArrayList<Relationship>();
		for(Relationship eachCommentRel: debate.getRelationships(DynamicRelationshipType.withName("Commented_On_Debate")))
			commentRelationsList.add(eachCommentRel);
		Collections.sort(commentRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachCommentRel: commentRelationsList)
			if(!commented_usersList.contains(eachCommentRel.getOtherNode(debate).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"), Direction.OUTGOING).getEndNode()))
				commented_usersList.add(eachCommentRel.getOtherNode(debate).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"), Direction.OUTGOING).getEndNode());
		
		//preparing json array for commented users
		JSONArray commentedUsersJSONArray = new JSONArray();
		if(commented_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("FN", commented_usersList.get(0).getProperty("first_name").toString()+" "+commented_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UN", commented_usersList.get(0).getProperty("user_name").toString());
			commentedUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("FN", commented_usersList.get(1).getProperty("first_name").toString()+" "+commented_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UN", commented_usersList.get(1).getProperty("user_name").toString());
			commentedUsersJSONArray.put(userJSON2);
		}
		else{
			for(Node votedUser: commented_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("FN", votedUser.getProperty("first_name").toString()+" "+votedUser.getProperty("last_name").toString());
				userJSON.put("UN", votedUser.getProperty("user_name").toString());
				commentedUsersJSONArray.put(userJSON);
			}
		}

		//prepare the list of asked debate question users
		ArrayList<Node> asked_usersList = new ArrayList<Node>();
		ArrayList<Relationship> askedRelationsList = new ArrayList<Relationship>();
		for(Relationship eachQuestionRel: debate.getRelationships(DynamicRelationshipType.withName("Asked_Debate_Question")))
			askedRelationsList.add(eachQuestionRel);
		Collections.sort(askedRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachQuestionRel: askedRelationsList)
			if(!asked_usersList.contains(eachQuestionRel.getOtherNode(debate)))
				asked_usersList.add(eachQuestionRel.getOtherNode(debate));
		
		//preparing json array for asked debate questions users
		JSONArray askedUsersJSONArray = new JSONArray();
		if(asked_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("Name", asked_usersList.get(0).getProperty("first_name").toString()+" "+asked_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UName", asked_usersList.get(0).getProperty("user_name").toString());
			askedUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("Name", asked_usersList.get(1).getProperty("first_name").toString()+" "+asked_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UName", asked_usersList.get(1).getProperty("user_name").toString());
			askedUsersJSONArray.put(userJSON2);
			number_of_asks_lessTwo = asked_usersList.size() - 2;
		}
		else{
			for(Node askedUser: asked_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("Name", askedUser.getProperty("first_name").toString()+" "+askedUser.getProperty("last_name").toString());
				userJSON.put("UName", askedUser.getProperty("user_name").toString());
				askedUsersJSONArray.put(userJSON);
			}
			number_of_asks_lessTwo = asked_usersList.size();
		}
				
		boolean isMarkedReadLater = false;
		if(userNode != null)
			for(Relationship readLaterRel: debate.getRelationships(DynamicRelationshipType.withName("debate_readlater")))
				if(userNode.equals(readLaterRel.getOtherNode(debate)))
				{
					isMarkedReadLater = true;
					break;
				}
		
/*		String category = "";   */
		
		String sub_category = ""; //nothing but hashtag
		for(Relationship belongs_to_subcategory_relation: debate.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Debate")))
			if(belongs_to_subcategory_relation.getOtherNode(debate).hasProperty("main"))
			{
				sub_category = belongs_to_subcategory_relation.getOtherNode(debate).getProperty("name").toString();
				break;
			}
		
		//get list of spaces tagged to this article
		ArrayList<Node> taggedSpaces = new ArrayList<Node>();
		StringBuffer space_title_list = new StringBuffer();
		StringBuffer space_title_id_list = new StringBuffer();
		for(Relationship space_tagged_rel: debate.getRelationships(DynamicRelationshipType.withName("Debate_Tagged_To_Space")))
		{
			taggedSpaces.add(space_tagged_rel.getOtherNode(debate));
			space_title_list.append(space_tagged_rel.getOtherNode(debate).getProperty("space_title").toString()+",");
			space_title_id_list.append(space_tagged_rel.getOtherNode(debate).getProperty("space_title_id").toString()+",");
		}
		
		json.put("P_CELEBRITY_UNAME", "");
		json.put("P_MODERATOR_UNAME", "");
		json.put("P_MODERATOR_FULLNAME", "");
		json.put("UST", author.getProperty("acc_type").toString());
		json.put("P_Language", debate.getProperty("lang").toString());
		json.put("P_EventEndTime", debate.getProperty("d_duration").toString());
		json.put("Comment_Count_Unique", commented_usersList.size());
		json.put("ev", 4);
		json.put("v_users", askedUsersJSONArray);
		json.put("votes", number_of_asks_lessTwo);
		json.put("Commented_Users", commentedUsersJSONArray);
		json.put("Comment_Count", commentRelationsList.size());
		json.put("Is_Neo4j", true);
		json.put("P_Pin", isPinned);
		json.put("P_Id", debate.getProperty("d_id").toString());
		json.put("P_Author", author.getProperty("user_name").toString());
		json.put("P_Author_FullName", author.getProperty("first_name").toString()+" "+author.getProperty("last_name").toString());
		json.put("P_IsMarkedReadLater", isMarkedReadLater);
		json.put("P_Title", debate.getProperty("d_title").toString());
		json.put("P_Title_ID", debate.getProperty("d_title_id").toString());
//		json.put("P_Category", category);
		json.put("P_SubCategory", sub_category);
		json.put("P_Num_Comments", commentRelationsList.size());
		json.put("P_Feature_Image", debate.getProperty("d_img_url").toString());
		json.put("P_Smry", debate.getProperty("d_content").toString());
		json.put("P_TimeCreated", debate.getProperty("time_created").toString());
		json.put("P_EventLocation", "");
		json.put("P_EventStartTime", debate.getProperty("d_date").toString());
		json.put("P_EventAttendStatus", false);
		json.put("P_SignsRequired", "");
		json.put("P_PetitionSignStatus", false);
		json.put("Space_Title", space_title_list.toString());
		json.put("Space_TitleId", space_title_id_list.toString());
		
		return json;
	}
	
	private JSONObject getTownhallJSONForTile(Node townhall, boolean isPinned, Node userNode)
	{
		JSONObject json = new JSONObject();
		
		int number_of_asks_lessTwo = 0;
		
		Node author = townhall.getSingleRelationship(DynamicRelationshipType.withName("Townhall_Written_By"),Direction.OUTGOING).getEndNode();
		
		//prepare the list of commented users (unique)
		ArrayList<Node> commented_usersList = new ArrayList<Node>();
		ArrayList<Relationship> commentRelationsList = new ArrayList<Relationship>();
		for(Relationship eachCommentRel: townhall.getRelationships(DynamicRelationshipType.withName("Commented_On_Townhall")))
			commentRelationsList.add(eachCommentRel);
		Collections.sort(commentRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachCommentRel: commentRelationsList)
			if(!commented_usersList.contains(eachCommentRel.getOtherNode(townhall).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"), Direction.OUTGOING).getEndNode()))
				commented_usersList.add(eachCommentRel.getOtherNode(townhall).getSingleRelationship(DynamicRelationshipType.withName("Comment_Written_By"), Direction.OUTGOING).getEndNode());
		
		//preparing json array for commented users
		JSONArray commentedUsersJSONArray = new JSONArray();
		if(commented_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("FN", commented_usersList.get(0).getProperty("first_name").toString()+" "+commented_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UN", commented_usersList.get(0).getProperty("user_name").toString());
			commentedUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("FN", commented_usersList.get(1).getProperty("first_name").toString()+" "+commented_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UN", commented_usersList.get(1).getProperty("user_name").toString());
			commentedUsersJSONArray.put(userJSON2);
		}
		else{
			for(Node votedUser: commented_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("FN", votedUser.getProperty("first_name").toString()+" "+votedUser.getProperty("last_name").toString());
				userJSON.put("UN", votedUser.getProperty("user_name").toString());
				commentedUsersJSONArray.put(userJSON);
			}
		}

		//prepare the list of asked townhall question users
		ArrayList<Node> asked_usersList = new ArrayList<Node>();
		ArrayList<Relationship> askedRelationsList = new ArrayList<Relationship>();
		for(Relationship eachQuestionRel: townhall.getRelationships(DynamicRelationshipType.withName("Asked_Question")))
			askedRelationsList.add(eachQuestionRel);
		Collections.sort(askedRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachQuestionRel: askedRelationsList)
			if(!asked_usersList.contains(eachQuestionRel.getOtherNode(townhall)))
				asked_usersList.add(eachQuestionRel.getOtherNode(townhall));
		
		//preparing json array for asked townhall questions users
		JSONArray askedUsersJSONArray = new JSONArray();
		if(asked_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("Name", asked_usersList.get(0).getProperty("first_name").toString()+" "+asked_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UName", asked_usersList.get(0).getProperty("user_name").toString());
			askedUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("Name", asked_usersList.get(1).getProperty("first_name").toString()+" "+asked_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UName", asked_usersList.get(1).getProperty("user_name").toString());
			askedUsersJSONArray.put(userJSON2);
			number_of_asks_lessTwo = asked_usersList.size() - 2;
		}
		else{
			for(Node askedUser: asked_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("Name", askedUser.getProperty("first_name").toString()+" "+askedUser.getProperty("last_name").toString());
				userJSON.put("UName", askedUser.getProperty("user_name").toString());
				askedUsersJSONArray.put(userJSON);
			}
			number_of_asks_lessTwo = asked_usersList.size();
		}
				
		boolean isMarkedReadLater = false;
		if(userNode != null)
			for(Relationship readLaterRel: townhall.getRelationships(DynamicRelationshipType.withName("townhall_readlater")))
				if(userNode.equals(readLaterRel.getOtherNode(townhall)))
				{
					isMarkedReadLater = true;
					break;
				}
		
/*		String category = "";	 */
		
		String sub_category = ""; //nothing but hashtag
		for(Relationship belongs_to_subcategory_relation: townhall.getRelationships(DynamicRelationshipType.withName("Belongs_To_Subcategory_Townhall")))
			if(belongs_to_subcategory_relation.getOtherNode(townhall).hasProperty("main"))
			{
				sub_category = belongs_to_subcategory_relation.getOtherNode(townhall).getProperty("name").toString();
				break;
			}
		
		//get list of spaces tagged to this townhall
		ArrayList<Node> taggedSpaces = new ArrayList<Node>();
		StringBuffer space_title_list = new StringBuffer();
		StringBuffer space_title_id_list = new StringBuffer();
		for(Relationship space_tagged_rel: townhall.getRelationships(DynamicRelationshipType.withName("Townhall_Tagged_To_Space")))
		{
			taggedSpaces.add(space_tagged_rel.getOtherNode(townhall));
			space_title_list.append(space_tagged_rel.getOtherNode(townhall).getProperty("space_title").toString()+",");
			space_title_id_list.append(space_tagged_rel.getOtherNode(townhall).getProperty("space_title_id").toString()+",");
		}
		
		String moderator_userName = "";
		String moderator_fullName = "";
		for(Relationship moderatedBy: townhall.getRelationships(DynamicRelationshipType.withName("Townhall_Moderated_By")))
		{
			moderator_userName = moderatedBy.getOtherNode(townhall).getProperty("user_name").toString();
			moderator_userName = moderatedBy.getOtherNode(townhall).getProperty("user_name").toString() + " " + moderatedBy.getOtherNode(townhall).getProperty("user_name").toString();
			break;
		}
		
		json.put("P_CELEBRITY_UNAME", townhall.getSingleRelationship(DynamicRelationshipType.withName("Townhall_Of"),Direction.OUTGOING).getOtherNode(townhall).getProperty("user_name").toString());
		json.put("P_MODERATOR_UNAME", moderator_userName);
		json.put("P_MODERATOR_FULLNAME", moderator_fullName);
		json.put("UST", author.getProperty("acc_type").toString());
		json.put("P_Language", townhall.getProperty("lang").toString());
		json.put("P_EventEndTime", townhall.getProperty("t_duration").toString());
		json.put("Comment_Count_Unique", commented_usersList.size());
		json.put("ev", 3);
		json.put("v_users", askedUsersJSONArray);
		json.put("votes", number_of_asks_lessTwo);
		json.put("Commented_Users", commentedUsersJSONArray);
		json.put("Comment_Count", commentRelationsList.size());
		json.put("Is_Neo4j", true);
		json.put("P_Pin", isPinned);
		json.put("P_Id", townhall.getProperty("t_id").toString());
		json.put("P_Author", author.getProperty("user_name").toString());
		json.put("P_Author_FullName", author.getProperty("first_name")+" "+author.getProperty("last_name").toString());
		json.put("P_IsMarkedReadLater", isMarkedReadLater);
		json.put("P_Title", townhall.getProperty("t_title").toString());
		json.put("P_Title_ID", townhall.getProperty("t_title_id").toString());
//		json.put("P_Category", category);
		json.put("P_SubCategory", sub_category);
		json.put("P_Num_Comments", commentRelationsList.size());
		json.put("P_Feature_Image", townhall.getProperty("t_img_url").toString());
		json.put("P_Smry", townhall.getProperty("t_content").toString());
		json.put("P_TimeCreated", townhall.getProperty("time_created").toString());
		json.put("P_EventLocation", "");
		json.put("P_EventStartTime", townhall.getProperty("t_date").toString());
		json.put("P_EventAttendStatus", false);
		json.put("P_SignsRequired", "");
		json.put("P_PetitionSignStatus", false);
		json.put("Space_Title", space_title_list.toString());
		json.put("Space_TitleId", space_title_id_list.toString());
		
		return json;
	}
	
	private JSONObject getSpaceJSONForTile(Node space, boolean isPinned, Node user)
	{
		JSONObject json = new JSONObject();
		
		int number_of_followers_lessTwo = 0;
		
		Node author = space.getSingleRelationship(DynamicRelationshipType.withName("Space_Created_By"),Direction.OUTGOING).getEndNode();
		
		//prepare the list of following users for the space
		ArrayList<Node> follow_usersList = new ArrayList<Node>();
		ArrayList<Relationship> followRelationsList = new ArrayList<Relationship>();
		for(Relationship eachFollowRel: space.getRelationships(DynamicRelationshipType.withName("Space_Followed_By")))
			followRelationsList.add(eachFollowRel);
		Collections.sort(followRelationsList, TimeCreatedComparatorForRelationships);
		for(Relationship eachFollowRel: followRelationsList)
			if(!follow_usersList.contains(eachFollowRel.getOtherNode(space)))
				follow_usersList.add(eachFollowRel.getOtherNode(space));
		
		//preparing json array for following users of space
		JSONArray followUsersJSONArray = new JSONArray();
		if(follow_usersList.size() >= 2){
			JSONObject userJSON1 = new JSONObject();
			userJSON1.put("Name", follow_usersList.get(0).getProperty("first_name").toString()+" "+follow_usersList.get(0).getProperty("last_name").toString());
			userJSON1.put("UName", follow_usersList.get(0).getProperty("user_name").toString());
			followUsersJSONArray.put(userJSON1);
			JSONObject userJSON2 = new JSONObject();
			userJSON2.put("Name", follow_usersList.get(1).getProperty("first_name").toString()+" "+follow_usersList.get(1).getProperty("last_name").toString());
			userJSON2.put("UName", follow_usersList.get(1).getProperty("user_name").toString());
			followUsersJSONArray.put(userJSON2);
			number_of_followers_lessTwo = follow_usersList.size() - 2;
		}
		else{
			for(Node followUser: follow_usersList)
			{
				JSONObject userJSON = new JSONObject();
				userJSON.put("Name", followUser.getProperty("first_name").toString()+" "+followUser.getProperty("last_name").toString());
				userJSON.put("UName", followUser.getProperty("user_name").toString());
				followUsersJSONArray.put(userJSON);
			}
			number_of_followers_lessTwo = follow_usersList.size();
		}
		
		json.put("P_CELEBRITY_UNAME", "");
		json.put("P_MODERATOR_UNAME", "");
		json.put("P_MODERATOR_FULLNAME", "");
		json.put("UST", author.getProperty("acc_type").toString());
		json.put("P_Language", "");
		json.put("P_EventEndTime", "");
		json.put("Comment_Count_Unique", 0);
		json.put("ev", 5);
		json.put("v_users", followUsersJSONArray);
		json.put("votes", number_of_followers_lessTwo);
		json.put("Commented_Users", "");
		json.put("Comment_Count", 0);
		json.put("Is_Neo4j", true);
		json.put("P_Pin", isPinned);
		json.put("P_Id", space.getProperty("space_id").toString());
		json.put("P_Author", author.getProperty("user_name").toString());
		json.put("P_Author_FullName", author.getProperty("first_name").toString()+" "+author.getProperty("last_name").toString());
		json.put("P_IsMarkedReadLater", false);
		json.put("P_Title", space.getProperty("space_title").toString());
		json.put("P_Title_ID", space.getProperty("space_title_id").toString());
//		json.put("P_Category", "");
		json.put("P_SubCategory", "");
		json.put("P_Num_Comments", "");
		json.put("P_Feature_Image", space.getProperty("space_featured_img").toString());
		json.put("P_Smry", space.getProperty("space_tagline").toString());
		json.put("P_TimeCreated", space.getProperty("time_created").toString());
		json.put("P_EventLocation", "");
		json.put("P_EventStartTime", "");
		json.put("P_EventAttendStatus", false);
		json.put("P_SignsRequired", "");
		json.put("P_PetitionSignStatus", "");
		json.put("Space_Title", space.getProperty("space_title").toString());
		json.put("Space_TitleId", space.getProperty("space_title_id").toString());
		
		return json;
	}
	
}
