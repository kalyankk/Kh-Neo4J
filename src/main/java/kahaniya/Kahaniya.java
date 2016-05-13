package kahaniya;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

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
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;

import scala.collection.immutable.List;
import kahaniya.KahaniyaCustomException;

public class Kahaniya implements KahaniyaService.Iface{

	
	//Index names
	public static final String GENRE_NAME_INDEX = "genre_index_by_name";
	public static final String LANG_NAME_INDEX = "language_index_by_name";
	public static final String GENRE_LANG_INDEX = "genre_language_index_by_name";
	public static final String USER_ID_INDEX = "user_index_by_id";
	public static final String USER_NAME_INDEX = "user_index_by_uname";
	public static final String USER_EMAIL_INDEX = "user_index_by_email";
	public static final String COMMENT_ID_INDEX = "comment_index_by_id";

	public static final String SERIES_ID_INDEX = "series_index_by_id";
	public static final String REVIEW_ID_INDEX = "review_index_by_id";
	public static final String CHAPTER_ID_INDEX = "chapter_index_by_id";
	public static final String CHAPTER_TITLE_ID_INDEX = "chapter_index_by_title_id";
	public static final String SERIES_TITLE_ID_INDEX = "series_index_by_title_id";
	public static final String SERIES_TYPE_INDEX = "series_index_by_type";
	
	public static final String USER_VIEWED_CHAPTER_REL_INDEX = "user_viewed_chapter_relation_index";

	public static final String KEYWORD_INDEX = "keyword_index_by_name";
	
	public static final String SEARCH_INDEX = "search_index";
	
	public static final String LOCK_INDEX = "lock_index";
	
	//search related keys
	public static final String SEARCH_USER = "search_user";
	public static final String SEARCH_CHAPTER = "search_chapter";
	public static final String SEARCH_SERIES = "search_series";
	
	//lock related keys
	public static final String LockName = "lock_node";

	//review related keys
	public static final String REVIEW_ID = "review_id";
	public static final String REVIEW_DATA = "review_data";
	
	//chapter related keys
	public static final String CHAPTER_ID = "chapter_id";
	public static final String CHAPTER_TITLE = "chapter_title";
	public static final String CHAPTER_TITLE_ID = "chapter_title_id";
	public static final String CHAPTER_FEAT_IMAGE = "chapter_feat_image";
	public static final String CHAPTER_FREE_OR_PAID = "chapter_free_or_paid";
	
	//chapter - user relationship properties
	public static final String CHAPTER_RATING = "chapter_rating";
	public static final String CHAPTER_VIEWS = "chapter_views";
	
	//user viewed a chapter related key
	public static final String USER_VIEWED_A_CHAPTER_ID = "user_viewer_a_chapter_id";
		
	//user related keys
	public static final String USER_ID = "user_id";
	public static final String USER_NAME = "user_name";
	public static final String FULL_NAME = "full_name";
	public static final String EMAIL = "email";
	public static final String MOBILE_NUMBER = "mobile_number";
	public static final String MOBILE_DIAL_CODE = "mobile_dial_code";
	public static final String DOB = "dob";
	public static final String GENDER = "gender";
	public static final String ADDRESS = "address";
	public static final String BIO = "bio";
	public static final String PRIVILEGE = "privilege";
	public static final String STATUS = "status";
	public static final String TIME_CREATED = "time_created";
	public static final String TIME_EDITED = "time_edited";
	public static final String IS_DELETED = "is_deleted";
	
	//genre node related keys
	public static final String GENRE_NAME = "genre_name";

	//language node related keys
	public static final String LANG_NAME = "language_name";
	
	//genre_language node related keys
	public static final String GENRE_LANG_NAME = "genre_language_name";
	
	//keyword node related keys
	public static final String KEYWORD_NAME = "keyword_name";
	
	
	//series node related keys
	public static final String SERIES_ID = "series_id";
	public static final String SERIES_TITLE = "series_title";
	public static final String SERIES_TITLE_ID = "series_title_id";
	public static final String SERIES_TAG_LINE = "series_tag_line";
	public static final String SERIES_FEAT_IMG = "series_feat_img";
	public static final String SERIES_KEYWORDS = "series_keywords";
	public static final String SERIES_COPYRIGHTS = "series_copyrights";
	public static final String SERIES_DD_IMG = "series_dd_img";
	public static final String SERIES_DD_SUMMARY = "series_dd_summary";
	public static final String SERIES_TYPE = "series_type";
	
	//comment related keys
	public static final String COMMENT_ID = "comment_id";
	public static final String COMMENT_CONTENT = "comment_content";
	
	public static final String NODE_TYPE = "node_type";
	public static final String USER_NODE = "user_node";
	public static final String GENRE_NODE = "genre_node";
	public static final String LANG_NODE = "language_node";
	public static final String GENRE_LANG_NODE = "genre_language_node";
	public static final String SERIES_NODE = "series_node";
	public static final String KEYWORD_NODE = "keyword_node";
	public static final String REVIEW_NODE = "review_node";
	public static final String CHAPTER_NODE = "chapter_node";
	public static final String COMMENT_NODE = "comment_node";

	
	private static GraphDatabaseService graphDb = null;
	
	
	//Relationship names
	public static final RelationshipType USER_INTERESTED_GENRE = DynamicRelationshipType.withName("USER_INTERESTED_GENRE");
	public static final RelationshipType USER_INTERESTED_LANGUAGE = DynamicRelationshipType.withName("USER_INTERESTED_LANGUAGE");
	public static final RelationshipType USER_FOLLOW_USER = DynamicRelationshipType.withName("USER_FOLLOW_USER");
	public static final RelationshipType USER_STARTED_SERIES = DynamicRelationshipType.withName("USER_STARTED_SERIES");

	public static final RelationshipType SERIES_BELONGS_TO_GENRE = DynamicRelationshipType.withName("SERIES_BELONGS_TO_GENRE");
	public static final RelationshipType SERIES_BELONGS_TO_LANGUAGE = DynamicRelationshipType.withName("SERIES_BELONGS_TO_LANGUAGE");
	public static final RelationshipType SERIES_BELONGS_TO_GENRE_LANGUAGE = DynamicRelationshipType.withName("SERIES_BELONGS_TO_GENRE_LANGUAGE");
	public static final RelationshipType SERIES_KEYWORD = DynamicRelationshipType.withName("SERIES_KEYWORD");
	
	public static final RelationshipType USER_WRITTEN_A_REVIEW = DynamicRelationshipType.withName("USER_WRITTEN_A_REVIEW");
	public static final RelationshipType REVIEW_BELONGS_TO_SERIES = DynamicRelationshipType.withName("REVIEW_BELONGS_TO_SERIES");
	public static final RelationshipType USER_SUBSCRIBED_TO_SERIES = DynamicRelationshipType.withName("USER_SUBSCRIBED_TO_SERIES");	
	public static final RelationshipType USER_FAV_CHAPTER = DynamicRelationshipType.withName("USER_FAV_CHAPTER");
	public static final RelationshipType USER_BOOKMARK_CHAPTER = DynamicRelationshipType.withName("USER_BOOKMARK_CHAPTER");
	public static final RelationshipType USER_RATED_A_CHAPTER = DynamicRelationshipType.withName("USER_RATED_A_CHAPTER");
	public static final RelationshipType USER_VIEWED_A_CHAPTER = DynamicRelationshipType.withName("USER_VIEWED_A_CHAPTER");	
	public static final RelationshipType USER_WRITTEN_A_CHAPTER = DynamicRelationshipType.withName("USER_WRITTEN_A_CHAPTER");	
	public static final RelationshipType CHAPTER_BELONGS_TO_SERIES = DynamicRelationshipType.withName("CHAPTER_BELONGS_TO_SERIES");	

	public static final RelationshipType USER_WRITTEN_A_COMMENT = DynamicRelationshipType.withName("USER_WRITTEN_A_COMMENT");
	public static final RelationshipType COMMENT_WRITTEN_ON_CHAPTER = DynamicRelationshipType.withName("COMMENT_WRITTEN_ON_CHAPTER");
	public static final RelationshipType REPLY_COMMENT_WRITTEN_ON_COMMENT = DynamicRelationshipType.withName("REPLY_COMMENT_WRITTEN_ON_COMMENT");	
	

	private static Comparator<Node> TimeCreatedComparatorForNodes = new Comparator<Node>() {
		public int compare(Node n1, Node n2) {
		   int v1 = 0;
		   int v2 = 0;
		   if(n1.hasProperty(TIME_CREATED))
			   v1 = Integer.parseInt(n1.getProperty(TIME_CREATED).toString());
		   if(n2.hasProperty(TIME_CREATED))
			   v2 = Integer.parseInt(n2.getProperty(TIME_CREATED).toString());
		   //ascending order
		   //return v1-v2;
		   //descending order
		   return v2-v1;
	    }};
	    
	private static Comparator<Node> TrendingComparatorForChapterNodes = new Comparator<Node>() {
		public int compare(Node n1, Node n2) {
		   int v1 = 0;
		   int v2 = 0;
		   int t = (int)(new Date().getTime()/1000) - (24*60*60);
		   if(n1.hasRelationship(USER_VIEWED_A_CHAPTER))
		   {
			   Iterator<Relationship> views = n1.getRelationships(USER_VIEWED_A_CHAPTER).iterator();
			   
			   while(views.hasNext())
			   {
				   Relationship rel = views.next();
				   if(Integer.parseInt(rel.getProperty(TIME_CREATED).toString()) >= t)
					   v1++;
			   }
		   }
		   if(n2.hasRelationship(USER_VIEWED_A_CHAPTER))
		   {
			   Iterator<Relationship> views = n2.getRelationships(USER_VIEWED_A_CHAPTER).iterator();
			   
			   while(views.hasNext())
			   {
				   Relationship rel = views.next();
				   if(Integer.parseInt(rel.getProperty(TIME_CREATED).toString()) >= t)
					   v2++;
			   }
		   }
		   //ascending order
		   //return v1-v2;
		   //descending order
		   return v2-v1;
	    }};
		
	private static Comparator<Node> TrendingComparatorForSeriesNodes = new Comparator<Node>() {
		public int compare(Node n1, Node n2) {
		   int v1 = 0;
		   int v2 = 0;
		   int t = (int)(new Date().getTime()/1000) - (24*60*60);
		   Iterator<Relationship> chaptersItr1 = n1.getRelationships(CHAPTER_BELONGS_TO_SERIES).iterator();
		   while(chaptersItr1.hasNext())
		   {
			   Node chapter = chaptersItr1.next().getStartNode();
			   if(chapter.hasRelationship(USER_VIEWED_A_CHAPTER))
			   {
				   Iterator<Relationship> views = chapter.getRelationships(USER_VIEWED_A_CHAPTER).iterator();
				   
				   while(views.hasNext())
				   {
					   Relationship rel = views.next();
					   if(Integer.parseInt(rel.getProperty(TIME_CREATED).toString()) >= t)
						   v1++;
				   }
			   }
		   }
		   Iterator<Relationship> chaptersItr2 = n2.getRelationships(CHAPTER_BELONGS_TO_SERIES).iterator();
		   while(chaptersItr2.hasNext()) 
		   {
			   Node chapter = chaptersItr2.next().getStartNode();
			   if(chapter.hasRelationship(USER_VIEWED_A_CHAPTER))
			   {
				   Iterator<Relationship> views = chapter.getRelationships(USER_VIEWED_A_CHAPTER).iterator();
			   
				   while(views.hasNext())
				   {
					   Relationship rel = views.next();
					   if(Integer.parseInt(rel.getProperty(TIME_CREATED).toString()) >= t)
						   v2++;
				   }
			   }
		   }
		   //ascending order
		   //return v1-v2;
		   //descending order
		   return v2-v1;
	    }};
			
	private static Comparator<Relationship> TimeCreatedComparatorForRelationships = new Comparator<Relationship>() {
			public int compare(Relationship n1, Relationship n2) {
			   int v1 = 0;
			   int v2 = 0;
			   if(n1.hasProperty(TIME_CREATED))
				   v1 = Integer.parseInt(n1.getProperty(TIME_CREATED).toString());
			   if(n2.hasProperty(TIME_CREATED))
				   v2 = Integer.parseInt(n2.getProperty(TIME_CREATED).toString());
			   //ascending order
			   //return v1-v2;
			   //descending order
			   return v2-v1;
		    }
		}; 

	private static Comparator<Node> TrendingComparatorForAuthorNodes = new Comparator<Node>() {
		public int compare(Node n1, Node n2) {
		   int v1 = 0;
		   int v2 = 0;
		   int t = (int)(new Date().getTime()/1000) - (30*24*60*60);
		   Iterator<Relationship> chaptersItr1 = n1.getRelationships(USER_WRITTEN_A_CHAPTER).iterator();
		   while(chaptersItr1.hasNext())
		   {
			   Relationship rel = chaptersItr1.next();
			   if(Integer.parseInt(rel.getProperty(TIME_CREATED).toString()) >= t)
						   v1++;
		   }
		   Iterator<Relationship> chaptersItr2 = n2.getRelationships(CHAPTER_BELONGS_TO_SERIES).iterator();
		   while(chaptersItr2.hasNext()) 
		   {
			   Relationship rel = chaptersItr2.next();
			   if(Integer.parseInt(rel.getProperty(TIME_CREATED).toString()) >= t)
						   v2++;

		   }
		   //ascending order
		   //return v1-v2;
		   //descending order
		   return v2-v1;
	    }};
				

    private Node User(String id, String full_name, String user_name, String email, String mobile, String dial_code, String dob, String gender, String address, String bio, int privilege, int status, int time_created){
		Node node = graphDb.createNode();
		node.setProperty(USER_ID,id);
	    node.setProperty(FULL_NAME,full_name);
		node.setProperty(USER_NAME,user_name);
		node.setProperty(EMAIL,email);
		node.setProperty(MOBILE_NUMBER,mobile);
		node.setProperty(MOBILE_DIAL_CODE, dial_code);
		node.setProperty(DOB,dob);
		node.setProperty(GENDER,gender);
		node.setProperty(ADDRESS,address);
		node.setProperty(BIO,bio);
		node.setProperty(PRIVILEGE,privilege);
		node.setProperty(STATUS,status);
	    node.setProperty(TIME_CREATED,time_created);
	    node.setProperty(NODE_TYPE, USER_NODE);
	    return node;
	}

    private Node Comment(String id, String content, int time_created){
		Node node = graphDb.createNode();
		node.setProperty(COMMENT_ID,id);
	    node.setProperty(COMMENT_CONTENT,content);
		node.setProperty(TIME_CREATED,time_created);
	    node.setProperty(NODE_TYPE, COMMENT_NODE);
	    return node;
	} 

    private Node Genre(String name){
		Node node = graphDb.createNode();
		node.setProperty(GENRE_NAME,name);
	    node.setProperty(NODE_TYPE, GENRE_NODE);
	    return node;
	} 

    private Node GenreLang(String genre_name, String lang_name){
		Node node = graphDb.createNode();
		node.setProperty(GENRE_NAME,genre_name);
		node.setProperty(LANG_NAME,lang_name);
	    node.setProperty(NODE_TYPE, GENRE_LANG_NODE);
	    return node;
	} 

    private Node Keyword(String name){
		Node node = graphDb.createNode();
		node.setProperty(KEYWORD_NAME,name);
	    node.setProperty(NODE_TYPE, KEYWORD_NODE);
	    return node;
	} 
    
    private Node Language(String name){
		Node node = graphDb.createNode();
		node.setProperty(LANG_NAME,name);
	    node.setProperty(NODE_TYPE, LANG_NODE);
	    return node;
	} 

	private Node Series(String series_id, String title, String title_id,
			String tag_line, String feature_image, String keywords,
			String copyrights, String dd_img, String dd_summary,
			int series_type, int time_created) {
		Node node = graphDb.createNode();
		node.setProperty(SERIES_ID, series_id);
		node.setProperty(SERIES_TITLE, title);
		node.setProperty(SERIES_TITLE_ID, title_id);
		node.setProperty(SERIES_TAG_LINE, tag_line);
		node.setProperty(SERIES_FEAT_IMG, feature_image);
		node.setProperty(SERIES_KEYWORDS, keywords);
		node.setProperty(SERIES_COPYRIGHTS, copyrights);
		node.setProperty(SERIES_DD_IMG, dd_img);
		node.setProperty(SERIES_DD_SUMMARY, dd_summary);
		node.setProperty(SERIES_TYPE, series_type);
		node.setProperty(NODE_TYPE, SERIES_NODE);
		node.setProperty(TIME_CREATED, time_created);
		
		return node;
	}
	
	private Node Review(String review_id, String data, int time_created)
	{
		Node node = graphDb.createNode();
		node.setProperty(REVIEW_ID, review_id);
		node.setProperty(REVIEW_DATA, data);
		node.setProperty(NODE_TYPE, REVIEW_NODE);
		return node;
	}

	private Node Chapter(String chapter_id, String title_id, String title,
			String feat_image, int free_or_paid, int time_created) {
		Node node = graphDb.createNode();
		node.setProperty(CHAPTER_ID, chapter_id);
		node.setProperty(CHAPTER_TITLE_ID, title_id);
		node.setProperty(CHAPTER_TITLE, title);
		node.setProperty(CHAPTER_FEAT_IMAGE, feat_image);
		node.setProperty(CHAPTER_FREE_OR_PAID, free_or_paid);
		node.setProperty(TIME_CREATED, time_created);
		node.setProperty(NODE_TYPE, CHAPTER_NODE);
		return node;
	}
	
	public void startThriftServer()
	{
		//starting thrift server		
		try{
			TServerSocket serverTransport = new TServerSocket(9777);
			KahaniyaService.Processor<KahaniyaService.Iface> processor = new KahaniyaService.Processor<KahaniyaService.Iface>(this);
			Args serverArgs = new Args(serverTransport);
			serverArgs.processor(processor);
			TServer server = new TThreadPoolServer(serverArgs);
			System.out.println("Kahaniya thrift service is started");
			server.serve();
			
		}catch(Exception e)
		{
			System.out.println(new Date().toString());
			e.printStackTrace();
		}
	}
	
	@Override
	public String jar_shutdown()
	{
		String res;
		try{
			System.out.println("Got the request to shut_down");
			Timer timer  = new Timer();
			TimerTask timerTask = new TimerTask () {
				    @Override
				    public void run () {
				      System.out.println("Jar will terminate in 1 minute");
				    	System.exit(0);
				    }
				};
				timer.schedule (timerTask, 60000);
				res = "Jar will terminate in 1 minute";
		}
		catch(Exception ex){
			System.out.println(new Date().toString());
		      System.out.println("Something went wrong, while jar_shutdown :"+ex.getMessage());
		      ex.printStackTrace();
		      res = "Failed to terminate";
		   }
		return res;
	}
	
	public Kahaniya()
	{
		if(graphDb == null)
			initGraphDb();
	
	}
	
	private static void initGraphDb()
	{
		//db path
		String storeDir = "/var/kahaniyaN4j/data/graph.db";
		
		//starting graph database with configuration
		graphDb = new GraphDatabaseFactory()
	    .newEmbeddedDatabaseBuilder( storeDir )
	    .setConfig(GraphDatabaseSettings.allow_store_upgrade, "true")
	    .setConfig(GraphDatabaseSettings.keep_logical_logs, "2 days")
	    .newGraphDatabase();
		
		//register safe shutdown while exiting
		registerShutdownHook( graphDb );
		
		//create indexes if any (node index and relation index)
		String[] nodeIndexNames = {
								GENRE_NAME_INDEX,
								LANG_NAME_INDEX,
								GENRE_LANG_INDEX,
								USER_ID_INDEX,
								USER_NAME_INDEX,
								USER_EMAIL_INDEX,
								COMMENT_ID_INDEX,
								SERIES_ID_INDEX,
								REVIEW_ID_INDEX,
								CHAPTER_ID_INDEX,
								CHAPTER_TITLE_ID_INDEX,
								SERIES_TITLE_ID_INDEX,
								SERIES_TYPE_INDEX,
								KEYWORD_INDEX,
								LOCK_INDEX,
								SEARCH_INDEX
									};	
		
		String[] relationshipIndexNames = {
								USER_VIEWED_CHAPTER_REL_INDEX
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
			for(String relationIndexName: relationshipIndexNames)
				graphDb.index().forRelationships(relationIndexName, customConfig);
				
			tx.success();
		}
		catch(Exception e){
			System.out.println(new Date().toString());
			System.out.println("Failed to create indexes");
			}
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
	
	public void add_neo4j_lock_nodes()
	{
		try (Transaction tx = graphDb.beginTx())
		{
			Index<Node> lockNodeIndex = graphDb.index().forNodes( LOCK_INDEX );
				
			//create lock node
				if(lockNodeIndex.get(LockName, LockName).getSingle() == null){ //create node if and only if there no locknode with given name
					Node lockNode = graphDb.createNode();  //creating a node
					lockNode.setProperty( LockName, LockName ); //attach name to lock node
					lockNodeIndex.add( lockNode, LockName, LockName ); //attach node to lock node index to retrieve later
				}
			
			tx.success();
		}
		catch(Exception e){
			System.out.println(new Date().toString());
			System.out.println("Exception @ add_neo4j_lock_nodes()");
			System.out.println("Failed to create lock nodes : " + e.getMessage());}
		finally{}
	}
	
	public void add_additional_properties() 
	{
		try (Transaction tx = graphDb.beginTx())
		{
			Index<Node> userNodeIndex = graphDb.index().forNodes( USER_ID_INDEX );
			Index<Node> seriesNodeIndex = graphDb.index().forNodes( SERIES_ID_INDEX );
			Index<Node> chapterNodeIndex = graphDb.index().forNodes( CHAPTER_ID_INDEX );
			Index<Node> searchIndex = graphDb.index().forNodes( SEARCH_INDEX );
				
			ResourceIterator<Node> userItr = userNodeIndex.query(USER_ID, "*").iterator();
			while(userItr.hasNext())
			{
				Node user = userItr.next();
				if(!user.hasProperty(MOBILE_DIAL_CODE))
					user.setProperty(MOBILE_DIAL_CODE, "");
				searchIndex.remove(user);
				searchIndex.add(user, SEARCH_USER, user.getProperty(USER_NAME).toString().toLowerCase() + " " + user.getProperty(FULL_NAME).toString().toLowerCase());
			}

			ResourceIterator<Node> seriesItr = seriesNodeIndex.query(SERIES_ID, "*").iterator();
			while(seriesItr.hasNext())
			{
				Node series = seriesItr.next();
				searchIndex.remove(series);
				searchIndex.add(series, SEARCH_SERIES, series.getProperty(SERIES_TITLE_ID).toString().toLowerCase());
			}
			
			ResourceIterator<Node> chapterItr = chapterNodeIndex.query(CHAPTER_ID, "*").iterator();
			while(chapterItr.hasNext())
			{
				Node chapter = chapterItr.next();
				searchIndex.remove(chapter);
				searchIndex.add(chapter, SEARCH_CHAPTER, chapter.getProperty(CHAPTER_TITLE_ID).toString().toLowerCase());
			}
			tx.success();
		}
		catch(Exception e){
			System.out.println(new Date().toString());
			System.out.println("Exception @ add_additional_properties()");
			System.out.println("Failed to add additional properties : " + e.getMessage());}
		finally{}
	}
	
	private Lock aquireWriteLock(Transaction tx) throws Exception {
		Index<Node> lockNodeIndex = graphDb.index().forNodes( LOCK_INDEX );
		Node tobeLockedNode = lockNodeIndex.get( LockName, LockName ).getSingle();
		if(tobeLockedNode == null)
	      throw new RuntimeException("Locking node for "+LockName+" not found, unbale to synchronize the call.");
		return tx.acquireWriteLock(tobeLockedNode);  //lock simultaneous execution of create_call to avoid duplicate creation
	}

	private boolean isRelationExistsBetween(RelationshipType relType, Node srcNode, Node targetNode)
	{
		Iterator<Relationship> itr = srcNode.getRelationships(Direction.OUTGOING, relType).iterator();
		while(itr.hasNext())
		{
			if(targetNode.equals(itr.next().getEndNode())) return true;
		}
		return false;
	}

	private void deleteRelation(RelationshipType relType, Node srcNode, Node targetNode)
	{
		Iterator<Relationship> itr = srcNode.getRelationships(Direction.OUTGOING, relType).iterator();
		while(itr.hasNext())
		{
			Relationship t_rel = itr.next();
			if(targetNode.equals(t_rel.getEndNode()))
			{
				t_rel.delete();
				return;
			}
		}
	}

	private Relationship createRelation(RelationshipType relType, Node srcNode, Node targetNode)
	{
		Relationship rel = srcNode.createRelationshipTo(targetNode, relType);
		rel.setProperty(TIME_CREATED, (int)(System.currentTimeMillis()/1000));
		return rel;
	}

	private Relationship createRelation(RelationshipType relType, Node srcNode, Node targetNode, int time_created)
	{
		Relationship rel = srcNode.createRelationshipTo(targetNode, relType);
		rel.setProperty(TIME_CREATED, time_created);
		return rel;
	}

	@Override
	public String create_genre(String name)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			Index<Node> genre_lang_index = graphDb.index().forNodes(GENRE_LANG_INDEX);
			
			if(name == null || name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string found for genre name");
			
			name = name.toLowerCase();
			
			if(genreName_index.get(GENRE_NAME,name).getSingle()!=null)
				throw new KahaniyaCustomException("Genre already exists with given name : "+name);
			
			Node genre_node = Genre(name);  // Creating a new genre node
			if(genre_node == null)
				throw new KahaniyaCustomException("Something went wrong, while creating genre with given name");

			//Indexing newly created genre node
			genreName_index.add(genre_node, GENRE_NAME, name.toLowerCase());
			
			//create genre_lang nodes for each language
			ResourceIterator<Node> langNodesItr = langName_index.query(LANG_NAME, "*").iterator();
			while(langNodesItr.hasNext())
			{
				Node langNode = langNodesItr.next();
				genre_lang_index.add(GenreLang(name, langNode.getProperty(LANG_NAME).toString()), GENRE_LANG_NAME, (name+" "+langNode.getProperty(LANG_NAME).toString()).toLowerCase());
			}
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_genre('"+name+"')");
			System.out.println("Something went wrong, while creating genre from create_genre  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_genre('"+name+"')");
			System.out.println("Something went wrong, while creating genre from create_genre  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}
	
	@Override
	public String edit_genre(String old_name, String new_name)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			Index<Node> genre_lang_index = graphDb.index().forNodes(GENRE_LANG_INDEX);

			if(old_name == null || old_name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string found for genre existing name");
			if(new_name == null || new_name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string found for new genre name");
			
			old_name = old_name.toLowerCase();
			new_name = new_name.toLowerCase();
						
			Node genre_node = genreName_index.get(GENRE_NAME,old_name.toLowerCase()).getSingle();
	
			if(genre_node == null)
				throw new KahaniyaCustomException("Genre doesnot exists with given old name");

			if(genreName_index.get(GENRE_NAME,new_name.toLowerCase()).getSingle() != null)
				throw new KahaniyaCustomException("Genre already exists with given new name");

			genre_node.setProperty(GENRE_NAME, old_name);
			//Update indexing for genre node
			genreName_index.remove(genre_node);
			genreName_index.add(genre_node, GENRE_NAME, new_name.toLowerCase());
			
			//update all genre_lang nodes
			//create genre_lang nodes for each language
			ResourceIterator<Node> langNodesItr = langName_index.query(LANG_NAME, "*").iterator();
			while(langNodesItr.hasNext())
			{
				Node langNode = langNodesItr.next();
				Node genre_lang_node = genre_lang_index.get(GENRE_LANG_NAME, (old_name + " " + langNode.getProperty(LANG_NAME).toString()).toLowerCase()).getSingle();
				if(genre_lang_node != null)
				{
					genre_lang_node.setProperty(GENRE_LANG_NAME, new_name + " " + langNode.getProperty(LANG_NAME).toString());
					genre_lang_index.remove(genre_lang_node);
					genre_lang_index.add(genre_lang_node, GENRE_LANG_NAME, (new_name+ " " + langNode.getProperty(LANG_NAME).toString()).toLowerCase());
				}
			}
			
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_genre('"+old_name+"','"+new_name+"')");
			System.out.println("Something went wrong, while editing genre from edit_genre  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_genre('"+old_name+"','"+new_name+"')");
			System.out.println("Something went wrong, while editing genre from edit_genre  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String delete_genre(String name)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			Index<Node> genre_lang_index = graphDb.index().forNodes(GENRE_LANG_INDEX);
			
			if(name == null || name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string found for genre name");
			
			name = name.toLowerCase();
			
			Node genre_node = genreName_index.get(GENRE_NAME,name.toLowerCase()).getSingle();
			
			if(genre_node == null)
				throw new KahaniyaCustomException("Genre node doesnot exists with given name");

			//Remove relationships for genre node
//			for(Relationship rel : genre_node.getRelationships())
//				rel.delete();

			if(genre_node.getDegree(SERIES_BELONGS_TO_GENRE) > 0)
				throw new KahaniyaCustomException("Genre node cannot be deleted, there exists some series related to given genre : " + name );
			
			//Remove genre_lang for all other lang
			ResourceIterator<Node> langNodesItr = langName_index.query(LANG_NAME, "*").iterator();
			while(langNodesItr.hasNext())
			{
				Node langNode = langNodesItr.next();
				Node genre_lang_node = genre_lang_index.get(GENRE_LANG_NAME, (name + " " + langNode.getProperty(LANG_NAME).toString()).toLowerCase()).getSingle();
				if(genre_lang_node != null)
				{
					genre_lang_index.remove(genre_lang_node);
					genre_lang_node.delete();
				}
			}

			
			//Remove indexing for genre node
			genreName_index.remove(genre_node);
			genre_node.delete();
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_genre('"+name+"')");
			System.out.println("Something went wrong, while deleting genre from delete_genre  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_genre('"+name+"')");
			System.out.println("Something went wrong, while deleting genre from delete_genre  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}
	
	@Override
	public String list_genres()
			throws TException {
		
		JSONArray res = new JSONArray();
		
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			
			ResourceIterator<Node> genre_nodes_itr = genreName_index.query(GENRE_NAME,"*").iterator();
			while(genre_nodes_itr.hasNext())
				res.put(getJSONForGenre(genre_nodes_itr.next()));
			
		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ list_genres()");
			System.out.println("Something went wrong, while returning genres  :"+ex.getMessage());
//			ex.printStackTrace();

		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ list_genres()");
			System.out.println("Something went wrong, while returning genres  :"+ex.getMessage());
			ex.printStackTrace();
		}
		
		return res.toString();
	}

	@Override
	public String create_language(String name)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> genre_lang_index = graphDb.index().forNodes(GENRE_LANG_INDEX);
			
			if(name == null || name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string found for language name");
			
			name = name.toLowerCase();
			
			if(langName_index.get(LANG_NAME,name.toLowerCase()).getSingle()!=null)
				throw new KahaniyaCustomException("Lang already exists with given name : "+name);

			Node lang_node = Language(name);  // Creating a new lang node
			if(lang_node == null)
				throw new KahaniyaCustomException("Something went wrong, while creating language with given name");

			//create genre_lang nodes for each language
			ResourceIterator<Node> genreNodesItr = genreName_index.query(GENRE_NAME, "*").iterator();
			while(genreNodesItr.hasNext())
			{
				Node genreNode = genreNodesItr.next();
				genre_lang_index.add(GenreLang(genreNode.getProperty(GENRE_NAME).toString(), name), GENRE_LANG_NAME, (genreNode.getProperty(GENRE_NAME).toString()+" "+name).toLowerCase());
			}
			

			//Indexing newly created lang node
			langName_index.add(lang_node, LANG_NAME, name.toLowerCase());
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_language('"+name+"')");
			System.out.println("Something went wrong, while creating language from create_language  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_language('"+name+"')");
			System.out.println("Something went wrong, while creating language from create_language  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}
	
	@Override
	public String edit_language(String old_name, String new_name)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> genre_lang_index = graphDb.index().forNodes(GENRE_LANG_INDEX);

			if(old_name == null || old_name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string found for old language name");
			if(new_name == null || new_name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string found for new language name");
			
			old_name = old_name.toLowerCase();
			new_name = new_name.toLowerCase();
			
			Node lang_node = langName_index.get(LANG_NAME,old_name.toLowerCase()).getSingle();
			
			if(lang_node == null)
				throw new KahaniyaCustomException("Lang doesnot exists with given old name");

			if(langName_index.get(LANG_NAME,new_name.toLowerCase()).getSingle() != null)
				throw new KahaniyaCustomException("Lang already exists with given new name");
			
			lang_node.setProperty(LANG_NAME, old_name);
			
			//update all genre_lang nodes
			//create genre_lang nodes for each language
			ResourceIterator<Node> genreNodesItr = genreName_index.query(GENRE_NAME, "*").iterator();
			while(genreNodesItr.hasNext())
			{
				Node genreNode = genreNodesItr.next();
				Node genre_lang_node = genre_lang_index.get(GENRE_LANG_NAME, (genreNode.getProperty(GENRE_NAME).toString()+" "+old_name).toLowerCase()).getSingle();
				if(genre_lang_node != null)
				{
					genre_lang_node.setProperty(GENRE_LANG_NAME, genreNode.getProperty(GENRE_NAME).toString()+" "+new_name);
					genre_lang_index.remove(genre_lang_node);
					genre_lang_index.add(genre_lang_node, GENRE_LANG_NAME, (genreNode.getProperty(GENRE_NAME).toString()+" "+new_name).toLowerCase());
				}
			}

			
			//Update indexing for lang node
			
			langName_index.remove(lang_node);
			langName_index.add(lang_node, LANG_NAME, new_name.toLowerCase());
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_language('"+old_name+"','"+new_name+"')");
			System.out.println("Something went wrong, while editing language from edit_language  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_language('"+old_name+"','"+new_name+"')");
			System.out.println("Something went wrong, while editing language from edit_language  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String delete_language(String name)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> genre_lang_index = graphDb.index().forNodes(GENRE_LANG_INDEX);

			if(name == null || name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string found for language name");
			
			name = name.toLowerCase();

			Node lang_node = langName_index.get(LANG_NAME,name.toLowerCase()).getSingle();
			
			if(lang_node == null)
				throw new KahaniyaCustomException("Lang node doesnot exists with given name");

			//Remove relationships for lang node
//			for(Relationship rel : lang_node.getRelationships())
//				rel.delete();
			if(lang_node.getDegree(SERIES_BELONGS_TO_LANGUAGE) > 0)
				throw new KahaniyaCustomException("Lang node cannot be deleted, there exists some series related to given lang : " + name );


			//Remove genre_lang for all other lang
			ResourceIterator<Node> genreNodesItr = genreName_index.query(GENRE_NAME, "*").iterator();
			while(genreNodesItr.hasNext())
			{
				Node genreNode = genreNodesItr.next();
				Node genre_lang_node = genre_lang_index.get(GENRE_LANG_NAME, (genreNode.getProperty(LANG_NAME).toString()+" "+name).toLowerCase()).getSingle();
				if(genre_lang_node != null)
				{
					genre_lang_index.remove(genre_lang_node);
					genre_lang_node.delete();
				}
			}
			
			//Remove indexing for lang node
			langName_index.remove(lang_node);
			lang_node.delete();
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_language('"+name+"')");
			System.out.println("Something went wrong, while deleting language from delete_language  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_language('"+name+"')");
			System.out.println("Something went wrong, while deleting language from delete_language  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}
	
	@Override
	public String list_languages()
			throws TException {
		
		JSONArray res = new JSONArray();
		
		try(Transaction tx = graphDb.beginTx())
		{
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			
			ResourceIterator<Node> lang_nodes_itr = langName_index.query(LANG_NAME,"*").iterator();
			while(lang_nodes_itr.hasNext())
				res.put(getJSONForLang(lang_nodes_itr.next()));
			
		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ list_languages()");
			System.out.println("Something went wrong, while returning languages  :"+ex.getMessage());
//			ex.printStackTrace();

		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ list_languages()");
			System.out.println("Something went wrong, while returning languages  :"+ex.getMessage());
			ex.printStackTrace();
		}
		
		return res.toString();
	}
	
	@Override
	public String create_user(String id, String full_name, String user_name,
			String email, String mobile_number, String dial_code, String dob, String gender,
			String address, String bio, String genres, String languages,
			int privilege, int status, int time_created)
			throws TException {

		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			
			if(id == null || id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the parameter id");
			if(full_name == null || full_name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the parameter full_name");
			if(email == null)
				email = "";
			if(mobile_number == null)
				mobile_number = "";
			if(dial_code == null)
				dial_code = "";
			if(dob == null)
				dob = "";
			if(gender == null)
				gender = "";
			if(address == null)
				address = "";
			if(bio == null)
				bio = "";
			if(genres == null)
				genres = "";
			if(languages == null)
				languages = "";
			
			
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> userName_index = graphDb.index().forNodes(USER_NAME_INDEX);
			Index<Node> userEmail_index = graphDb.index().forNodes(USER_EMAIL_INDEX);
			Index<Node> search_index = graphDb.index().forNodes(SEARCH_INDEX);
			
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			
			if(userId_index.get(USER_ID,id).getSingle()!=null)
				throw new KahaniyaCustomException("User already exists with given id : "+id);
			if(userName_index.get(USER_NAME,user_name.toLowerCase()).getSingle()!=null)
				throw new KahaniyaCustomException("User already exists with given user_name : "+user_name);			
			
			if(!email.equals("") && userEmail_index.get(EMAIL,email.toLowerCase()).getSingle()!=null)
				throw new KahaniyaCustomException("User already exists with given email : "+email);
			
			Node user_node = User(id, full_name, user_name, email, mobile_number, dial_code, dob, gender, address, bio, privilege, status, time_created);  // Creating a new user node
			if(user_node == null)
				throw new KahaniyaCustomException("Something went wrong, while creating user ");

			//create relationships with Genres and Languages
			for(String genre_name : genres.split(","))
			{
				if(genreName_index.get(GENRE_NAME, genre_name.toLowerCase()).getSingle() != null)
					createRelation(USER_INTERESTED_GENRE, user_node, genreName_index.get(GENRE_NAME, genre_name.toLowerCase()).getSingle());
			}
			
			for(String lang_name : languages.split(","))
			{
				if(langName_index.get(LANG_NAME, lang_name.toLowerCase()).getSingle() != null)
					createRelation(USER_INTERESTED_LANGUAGE, user_node, langName_index.get(LANG_NAME, lang_name.toLowerCase()).getSingle());
			}
			
			//Indexing newly created user node
			userId_index.add(user_node, USER_ID, id);
			userName_index.add(user_node, USER_NAME, user_name.toLowerCase());
			search_index.add(user_node, SEARCH_USER, user_name.toLowerCase() + " " + full_name.toLowerCase());
			
			if(!email.equals(""))
				userEmail_index.add(user_node, EMAIL, email.toLowerCase());

			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_user()");
			System.out.println("Something went wrong, while creating user from create_user  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_user()");
			System.out.println("Something went wrong, while creating user from create_user  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	
	}

	@Override
	public String edit_user_basic_info(String id, String full_name,
			String gender, String dob) throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(id == null || id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param id");
			if(full_name == null || full_name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param full_name");
			if(gender == null)
				gender = "";
			if(dob == null)
				dob = "";
			
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> search_index = graphDb.index().forNodes(SEARCH_INDEX);
			
			Node user_node = userId_index.get(USER_ID,id).getSingle();
			if(user_node == null)
				throw new KahaniyaCustomException("User does not exists with given id : "+id);
			
			user_node.setProperty(FULL_NAME, full_name);
			user_node.setProperty(GENDER, gender);
			user_node.setProperty(DOB, dob);
			
			search_index.remove(user_node);
			search_index.add(user_node, SEARCH_USER, user_node.getProperty(USER_NAME).toString().toLowerCase() + " " + full_name.toLowerCase());
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_user_basic_info()");
			System.out.println("Something went wrong, while editing user from edit_user_basic_info  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_user_basic_info()");
			System.out.println("Something went wrong, while editing user from edit_user_basic_info  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	
	}

	@Override
	public String edit_user_contact_details(String id, String email,
			String mobile_number) throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(id == null || id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param id");
			if(email == null)
				email = "";
			if(mobile_number == null)
				mobile_number = "";

			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> userEmail_index = graphDb.index().forNodes(USER_EMAIL_INDEX);
			
			Node user_node = userId_index.get(USER_ID,id).getSingle();
			
			if(user_node == null)
				throw new KahaniyaCustomException("User does not exists with given id : "+id);
			
			
			if(!email.equals("") && userEmail_index.get(EMAIL,email.toLowerCase()).getSingle()!=null)
				throw new KahaniyaCustomException("User already exists with given email : "+email);
			
			
			user_node.setProperty(EMAIL, email);
			user_node.setProperty(MOBILE_NUMBER, mobile_number);
			
			userEmail_index.remove(user_node);
			if(!email.equals(""))userEmail_index.add(user_node, EMAIL, email.toLowerCase());
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_user_contact_details()");
			System.out.println("Something went wrong, while editing user from edit_user_contact_details  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_user_contact_details()");
			System.out.println("Something went wrong, while editing user from edit_user_contact_details  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}
	
	@Override
	public String edit_user_security_details(String id, String user_name)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(id == null || id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param id");
			if(user_name == null || user_name.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_name");
			
			
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> userName_index = graphDb.index().forNodes(USER_NAME_INDEX);
			Index<Node> search_index = graphDb.index().forNodes(SEARCH_INDEX);
			
			Node user_node = userId_index.get(USER_ID,id).getSingle();
			if(user_node == null)
				throw new KahaniyaCustomException("User does not exists with given id : "+id);
			
			user_node.setProperty(USER_NAME, user_name);
			
			userName_index.remove(user_node);
			userName_index.add(user_node, USER_NAME, user_name.toLowerCase());
			search_index.remove(user_node);
			search_index.add(user_node, SEARCH_USER, user_name.toLowerCase() + " " + user_node.getProperty(FULL_NAME).toString().toLowerCase());
						
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_user_security_details()");
			System.out.println("Something went wrong, while editing user from edit_user_security_details  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_user_security_details()");
			System.out.println("Something went wrong, while editing user from edit_user_security_details  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}
	
	@Override
	public String edit_user_languages(String id, String languages)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(id == null || id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param id");
			if(languages == null)
				languages = "";
			
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			
			Node user_node = userId_index.get(USER_ID,id).getSingle();
			if(user_node == null)
				throw new KahaniyaCustomException("User does not exists with given id : "+id);
			
			for(Relationship rel : user_node.getRelationships(USER_INTERESTED_LANGUAGE))
				rel.delete();

			for(String lang_name : languages.split(","))
			{
				if(langName_index.get(LANG_NAME, lang_name.toLowerCase()).getSingle() != null)
					createRelation(USER_INTERESTED_LANGUAGE, user_node, langName_index.get(LANG_NAME, lang_name.toLowerCase()).getSingle());
			}

			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_user_languages()");
			System.out.println("Something went wrong, while editing user from edit_user_languages  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_user_languages()");
			System.out.println("Something went wrong, while editing user from edit_user_languages  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}
	
	@Override
	public String edit_user_genres(String id, String genres)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(id == null || id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param id");
			if(genres == null)
				genres = "";
			
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			
			Node user_node = userId_index.get(USER_ID,id).getSingle();
			if(user_node == null)
				throw new KahaniyaCustomException("User does not exists with given id : "+id);
			
			for(Relationship rel : user_node.getRelationships(USER_INTERESTED_GENRE))
				rel.delete();

			for(String lang_name : genres.split(","))
			{
				if(genreName_index.get(GENRE_NAME, lang_name.toLowerCase()).getSingle() != null)
					createRelation(USER_INTERESTED_GENRE, user_node, genreName_index.get(GENRE_NAME, lang_name.toLowerCase()).getSingle());
			}

			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_user_genres()");
			System.out.println("Something went wrong, while editing user from edit_user_genres  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_user_genres()");
			System.out.println("Something went wrong, while editing user from edit_user_genres  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String follow_user(String user_id_1, String user_id_2, int time)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id_1 == null || user_id_1.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id_1");
			if(user_id_2 == null || user_id_2.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id_2");
			
			aquireWriteLock(tx);
			
			Index<Node> userIdIndex = graphDb.index().forNodes(USER_ID_INDEX);
			Node userOne = userIdIndex.get(USER_ID, user_id_1).getSingle();
			Node userTwo = userIdIndex.get(USER_ID, user_id_2).getSingle();

			if(userOne == null)
				throw new KahaniyaCustomException("User does not exists with given id : " + user_id_1);
			if(userTwo == null)
				throw new KahaniyaCustomException("User does not exists with given id : " + user_id_2);
			
			if(isRelationExistsBetween(USER_FOLLOW_USER, userOne, userTwo))
			{
				deleteRelation(USER_FOLLOW_USER, userOne, userTwo);
			}			
			else
			{
				createRelation(USER_FOLLOW_USER, userOne, userTwo, time);
			}

			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ follow_user()");
			System.out.println("Something went wrong, while following user from follow_user  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ follow_user()");
			System.out.println("Something went wrong, while following user from follow_user  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String deactivate_user(String user_id) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	private JSONObject getJSONForGenre(Node genre)
	{
		JSONObject ret = new JSONObject();
		
			JSONObject obj = new JSONObject();
			obj.put("name", genre.getProperty(GENRE_NAME).toString());
		ret.put("type", "genre");
		ret.put("Obj",obj);
		ret.put("Is_Neo4j",true);
		return ret;
	}

	private JSONObject getJSONForReview(Node review)
	{
		JSONObject ret = new JSONObject();
		ret.put("id", review.getProperty(REVIEW_ID).toString());
		ret.put("data",review.getProperty(REVIEW_DATA));
		ret.put("Is_Neo4j",true);
		return ret;
	}
	
	private JSONObject getJSONForLang(Node genre)
	{
		JSONObject ret = new JSONObject();
		
			JSONObject obj = new JSONObject();
			obj.put("name", genre.getProperty(LANG_NAME).toString());
		ret.put("type", "lang");
		ret.put("Obj",obj);
		ret.put("Is_Neo4j",true);
		return ret;
	}

	@Override
	public String create_or_edit_series(String series_id, String user_id,
			String title, String title_id, String tag_line,
			String feature_image, String genre, String language,
			String keywords, String copyrights, String dd_img,
			String dd_summary, int series_type, int time_created, int is_edit)
			throws TException {
		if(is_edit == 0)
			return create_series(series_id, user_id, title, title_id, tag_line, feature_image, genre, language, keywords, copyrights, dd_img, dd_summary, series_type, time_created);
		else if(is_edit == 1)
			return edit_series(series_id, user_id, title, title_id, tag_line, feature_image, genre, language, keywords, copyrights, dd_img, dd_summary, series_type, time_created);
		else return "false";
	}
	
	private String create_series(String series_id, String user_id,
			String title, String title_id, String tag_line,
			String feature_image, String genre, String language,
			String keywords, String copyrights, String dd_img,
			String dd_summary, int series_type, int time_created){

		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");
			if(title == null || title.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param title");
			if(title_id == null || title_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param title_id");
			if(genre == null || genre.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param genre");
			if(language == null || language.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param language");
			if(tag_line == null)
				tag_line = "";
			if(feature_image == null)
				feature_image = "";
			if(keywords == null)
				keywords = "";
			if(copyrights == null)
				copyrights = "";
			if(dd_img == null)
				dd_img = "";
			if(dd_summary == null)
				dd_summary = "";
			
			
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> seriesTitleId_index = graphDb.index().forNodes(SERIES_TITLE_ID_INDEX);
			Index<Node> seriesType_index = graphDb.index().forNodes(SERIES_TYPE_INDEX);

			Index<Node> search_index = graphDb.index().forNodes(SEARCH_INDEX);
			
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			Index<Node> genre_lang_index = graphDb.index().forNodes(GENRE_LANG_INDEX);
			
			Index<Node> keyword_index = graphDb.index().forNodes(KEYWORD_INDEX);
			
			Node userNode = userId_index.get(USER_ID,user_id).getSingle();
			if(userNode == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
			if(seriesId_index.get(SERIES_ID,series_id).getSingle()!=null)
				throw new KahaniyaCustomException("Series already exists with given id : "+series_id);
			if(seriesTitleId_index.get(SERIES_TITLE_ID,title_id.toLowerCase()).getSingle()!=null)
				throw new KahaniyaCustomException("Series already exists with given title id : "+title_id);

			Node genreNode = genreName_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
			if(genreNode == null)
				throw new KahaniyaCustomException("Genre doesnot exists for the name : " + genre);
			
			Node langNode = langName_index.get(LANG_NAME, language.toLowerCase()).getSingle();
			if(langNode == null)
				throw new KahaniyaCustomException("Language doesnot exists for the name : " + language);
	
			Node genre_lang_node = genre_lang_index.get(GENRE_LANG_NAME, genre.toLowerCase() + " " +language.toLowerCase()).getSingle();
			if(genre_lang_node == null)
				throw new KahaniyaCustomException("Genre + Language doesnot exists for the name : " + genre+" "+language);
			
			Node series_node = Series(series_id, title, title_id, tag_line, feature_image, keywords, copyrights, dd_img, dd_summary, series_type, time_created);  // Creating a new series node
			if(series_node == null)
				throw new KahaniyaCustomException("Something went wrong, while creating series ");

			//create relationship with user
			createRelation(USER_STARTED_SERIES, userNode, series_node);
			
			//create relationship with keywords
			keywords = keywords.toLowerCase();
			for(String keyword : keywords.split(","))
			{
				Node keyword_node = keyword_index.get(KEYWORD_NAME, keyword.toLowerCase()).getSingle();
				if(keyword_node == null)
				{
					keyword_node = Keyword(keyword);
					keyword_index.add(keyword_node, KEYWORD_NAME, keyword.toLowerCase());
				}
				createRelation(SERIES_KEYWORD, series_node, keyword_node);
				
			}
			//create relationships with Genres and Languages
			createRelation(SERIES_BELONGS_TO_GENRE, series_node, genreNode);
			createRelation(SERIES_BELONGS_TO_LANGUAGE, series_node, langNode);
			createRelation(SERIES_BELONGS_TO_GENRE_LANGUAGE, series_node, genre_lang_node);
			
			//Indexing newly created series node
			seriesId_index.add(series_node, SERIES_ID, series_id);
			seriesTitleId_index.add(series_node, SERIES_TITLE_ID, title_id.toLowerCase());
			seriesType_index.add(series_node, SERIES_TYPE, series_type);

			search_index.add(series_node, SEARCH_SERIES, title_id.toLowerCase());
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_series()");
			System.out.println("Something went wrong, while creating series from create_series  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_series()");
			System.out.println("Something went wrong, while creating series from create_series  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;

	}

	private String edit_series(String series_id, String user_id,
			String title, String title_id, String tag_line,
			String feature_image, String genre, String language,
			String keywords, String copyrights, String dd_img,
			String dd_summary, int series_type, int time_edited){

		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");
			if(title == null || title.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param title");
			if(title_id == null || title_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param title_id");
			if(genre == null || genre.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param genre");
			if(language == null || language.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param language");
			if(tag_line == null)
				tag_line = "";
			if(feature_image == null)
				feature_image = "";
			if(keywords == null)
				keywords = "";
			if(copyrights == null)
				copyrights = "";
			if(dd_img == null)
				dd_img = "";
			if(dd_summary == null)
				dd_summary = "";

			aquireWriteLock(tx);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> seriesType_index = graphDb.index().forNodes(SERIES_TYPE_INDEX);
			
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			Index<Node> genre_lang_index = graphDb.index().forNodes(GENRE_LANG_INDEX);

			Index<Node> keyword_index = graphDb.index().forNodes(KEYWORD_INDEX);
			
			Node series_node = seriesId_index.get(SERIES_ID, series_id).getSingle();
			if(series_node == null)
				throw new KahaniyaCustomException("Series doesnot exists with given id : "+series_id);
			
			Node genreNode = genreName_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
			if(genreNode == null)
				throw new KahaniyaCustomException("Genre doesnot exists for the name : " + genre);
			
			Node langNode = langName_index.get(LANG_NAME, language.toLowerCase()).getSingle();
			if(langNode == null)
				throw new KahaniyaCustomException("Language doesnot exists for the name : " + language);
			
			Node genre_lang_node = genre_lang_index.get(GENRE_LANG_NAME, genre.toLowerCase()+" "+language.toLowerCase()).getSingle();
			if(genre_lang_node == null)
				throw new KahaniyaCustomException("Genre Language doesnot exists for the name : " + genre+" "+language);
			
			//remove existing relationships with Genres, Languages and keywords
			for(Relationship rel : series_node.getRelationships(SERIES_BELONGS_TO_GENRE))
				rel.delete();
			for(Relationship rel : series_node.getRelationships(SERIES_BELONGS_TO_LANGUAGE))
				rel.delete();
			for(Relationship rel : series_node.getRelationships(SERIES_BELONGS_TO_GENRE_LANGUAGE))
				rel.delete();
			for(Relationship rel : series_node.getRelationships(SERIES_KEYWORD))
				rel.delete();

			//create relationships with Genres and Languages
			createRelation(SERIES_BELONGS_TO_GENRE, series_node, genreNode, Integer.parseInt(series_node.getProperty(TIME_CREATED).toString()));
			createRelation(SERIES_BELONGS_TO_LANGUAGE, series_node, langNode, Integer.parseInt(series_node.getProperty(TIME_CREATED).toString()));
			createRelation(SERIES_BELONGS_TO_GENRE_LANGUAGE, series_node, genre_lang_node, Integer.parseInt(series_node.getProperty(TIME_CREATED).toString()));
			
			//create relationship with keywords
			keywords = keywords.toLowerCase();
			for(String keyword : keywords.split(","))
			{
				Node keyword_node = keyword_index.get(KEYWORD_NAME, keyword.toLowerCase()).getSingle();
				if(keyword_node == null)
				{
					keyword_node = Keyword(keyword);
					keyword_index.add(keyword_node, KEYWORD_NAME, keyword.toLowerCase());
				}
				createRelation(SERIES_KEYWORD, series_node, keyword_node, Integer.parseInt(series_node.getProperty(TIME_CREATED).toString()));
				
			}
			
			series_node.setProperty(SERIES_TITLE, title);
			series_node.setProperty(SERIES_TAG_LINE, tag_line);
			series_node.setProperty(SERIES_FEAT_IMG, feature_image);
			series_node.setProperty(SERIES_KEYWORDS, keywords);
			series_node.setProperty(SERIES_COPYRIGHTS, copyrights);
			series_node.setProperty(SERIES_DD_IMG, dd_img);
			series_node.setProperty(SERIES_DD_SUMMARY, dd_summary);
			series_node.setProperty(SERIES_TYPE, series_type);
			
			//update indexing for the edited series node
			seriesType_index.remove(series_node);
			seriesType_index.add(series_node, SERIES_TYPE, series_type);

			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_series()");
			System.out.println("Something went wrong, while editing series from edit_series  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_series()");
			System.out.println("Something went wrong, while editing series from edit_series  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;

	}

	@Override
	public String create_or_edit_review(String series_id, String review_id,
			String data, String user_id, int time_created, int is_edit) throws TException {
		if(is_edit == 0)
			return create_review(series_id, review_id, data, user_id, time_created);
		else if(is_edit == 1)
			return edit_review(series_id, review_id, data, user_id, time_created);
		else return "false";		
	}
	
	public String create_review(String series_id, String review_id, String data, String user_id, int time_created)
	{
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");
			if(review_id == null || review_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param review_id");
			if(data == null || data.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param data");
			
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> reviewId_index = graphDb.index().forNodes(REVIEW_ID_INDEX);
			
			Node userNode = userId_index.get(USER_ID,user_id).getSingle();
			if(userNode == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
			Node seriesNode = seriesId_index.get(SERIES_ID,series_id).getSingle();
			if(seriesNode == null)
				throw new KahaniyaCustomException("Series doesnot exists with given id : "+series_id);
			
			if(reviewId_index.get(REVIEW_ID,review_id).getSingle()!=null)
				throw new KahaniyaCustomException("Review already exists with given id : "+review_id);
			
			Node review_node = Review(review_id, data, time_created);  // Creating a new review node
			if(review_node == null)
				throw new KahaniyaCustomException("Something went wrong, while creating review ");

			//create relationship with user
			createRelation(USER_WRITTEN_A_REVIEW, userNode, review_node, time_created);
			
			//create relationships with Series
			createRelation(REVIEW_BELONGS_TO_SERIES, review_node, seriesNode, time_created);
			
			//Indexing newly created series node
			reviewId_index.add(review_node, REVIEW_ID, review_id);
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_review()");
			System.out.println("Something went wrong, while creating review from create_review  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_review()");
			System.out.println("Something went wrong, while creating review from create_review  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	public String edit_review(String series_id, String review_id, String data, String user_id, int time_edited)
	{

		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");
			if(review_id == null || review_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param review_id");
			if(data == null || data.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param data");
			

			aquireWriteLock(tx);
			
			Index<Node> reviewId_index = graphDb.index().forNodes(REVIEW_ID_INDEX);
			Node review_node = reviewId_index.get(REVIEW_ID, review_id).getSingle();
			if(review_node == null)
				throw new KahaniyaCustomException("Review doesnot exists with given id : "+review_id);
			
			review_node.setProperty(REVIEW_DATA, data);

			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_review()");
			System.out.println("Something went wrong, while editing review from edit_review  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_review()");
			System.out.println("Something went wrong, while editing review from edit_review  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String create_or_edit_chapter(String chapter_id, String series_id,
			String series_type, String user_id, String title_id, String title,
			String feat_image, int time_created, int free_or_paid, int is_edit)
			throws TException {
		if(is_edit == 0)
			return create_chapter(chapter_id, series_id, series_type, user_id, title_id, title, feat_image, free_or_paid, time_created);
		else if(is_edit == 1)
			return edit_chapter(chapter_id, series_id, series_type, user_id, title_id, title, feat_image, free_or_paid, time_created);
		else return "false";		
	}
	
	private String edit_chapter(String chapter_id, String series_id,
			String series_type, String user_id, String title_id,
			String title, String feat_image, int free_or_paid, int time_created) {

		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");
			if(chapter_id == null || chapter_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param chapter_id");
			if(title_id == null || title_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param title_id");
			if(title == null || title.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param title");
			if(feat_image == null)
				feat_image = "";

			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			
			Node userNode = userId_index.get(USER_ID,user_id).getSingle();
			if(userNode == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
			Node seriesNode = seriesId_index.get(SERIES_ID,series_id).getSingle();
			if(seriesNode == null)
				throw new KahaniyaCustomException("Series doesnot exists with given id : "+series_id);

			Node chapterNode = chapterId_index.get(CHAPTER_ID,chapter_id).getSingle();
			if(chapterNode == null)
				throw new KahaniyaCustomException("Chapter doesnot exists with given id : "+chapter_id);

			chapterNode.setProperty(CHAPTER_TITLE, title);
			chapterNode.setProperty(CHAPTER_FEAT_IMAGE, feat_image);
			chapterNode.setProperty(CHAPTER_FREE_OR_PAID, free_or_paid);
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_chapter()");
			System.out.println("Something went wrong, while editing chapter from edit_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_chapter()");
			System.out.println("Something went wrong, while editing chapter from edit_chapter  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	private String create_chapter(String chapter_id, String series_id,
			String series_type, String user_id, String title_id,
			String title, String feat_image, int free_or_paid, int time_created) {
		
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");
			if(chapter_id == null || chapter_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param chapter_id");
			if(title_id == null || title_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param title_id");
			if(title == null || title.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param title");
			if(feat_image == null)
				feat_image = "";


			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> chapterTitleId_index = graphDb.index().forNodes(CHAPTER_TITLE_ID_INDEX);
			Index<Node> search_index = graphDb.index().forNodes(SEARCH_INDEX);
			
			Node userNode = userId_index.get(USER_ID,user_id).getSingle();
			if(userNode == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
			Node seriesNode = seriesId_index.get(SERIES_ID,series_id).getSingle();
			if(seriesNode == null)
				throw new KahaniyaCustomException("Series doesnot exists with given id : "+series_id);

			if(chapterId_index.get(CHAPTER_ID,chapter_id).getSingle()!=null)
				throw new KahaniyaCustomException("Chapter already exists with given id : "+chapter_id);
			
			if(chapterTitleId_index.get(CHAPTER_TITLE_ID,title_id.toLowerCase()).getSingle()!=null)
				throw new KahaniyaCustomException("Chapter already exists with given title id : "+title_id);
			
			Node chapter_node = Chapter(chapter_id, title_id, title, feat_image, free_or_paid, time_created);  // Creating a new chapter node
			if(chapter_node == null)
				throw new KahaniyaCustomException("Something went wrong, while creating chapter ");

			//create relationship with user
			createRelation(USER_WRITTEN_A_CHAPTER, userNode, chapter_node, time_created);
			
			//create relationships with Series
			createRelation(CHAPTER_BELONGS_TO_SERIES, chapter_node, seriesNode, time_created);
			
			//Indexing newly created series node
			chapterId_index.add(chapter_node, CHAPTER_ID, chapter_id);
			chapterTitleId_index.add(chapter_node, CHAPTER_TITLE_ID, title_id.toLowerCase());
			search_index.add(chapter_node, SEARCH_CHAPTER, title_id.toLowerCase());
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_chapter()");
			System.out.println("Something went wrong, while creating chapter from create_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_chapter()");
			System.out.println("Something went wrong, while creating chapter from create_chapter  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String subscribe_series(String series_id, String user_id, int time)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");

			aquireWriteLock(tx);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			
			Node series_node = seriesId_index.get(SERIES_ID, series_id).getSingle();
			if(series_node == null)
				throw new KahaniyaCustomException("Series doesnot exists with given id : "+series_id);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			if(user_node == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
			
			if(isRelationExistsBetween(USER_SUBSCRIBED_TO_SERIES, user_node, series_node))
				deleteRelation(USER_SUBSCRIBED_TO_SERIES, user_node, series_node);
			else
				createRelation(USER_SUBSCRIBED_TO_SERIES, user_node, series_node, time);
						
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ subscribe_series()");
			System.out.println("Something went wrong, while subscribing series from subscribe_series  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ subscribe_series()");
			System.out.println("Something went wrong, while subscribing series from subscribe_series  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String favourite_chapter(String chapter_id, String series_id,
			String user_id, int time) throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");
			if(chapter_id == null || chapter_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param chapter_id");

			aquireWriteLock(tx);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			
			Node chapter_node = chapterId_index.get(CHAPTER_ID, chapter_id).getSingle();
			if(chapter_node == null)
				throw new KahaniyaCustomException("Chapter doesnot exists with given id : "+chapter_id);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			if(user_node == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
			
			if(isRelationExistsBetween(USER_FAV_CHAPTER, user_node, chapter_node))
				deleteRelation(USER_FAV_CHAPTER, user_node, chapter_node);
			else
				createRelation(USER_FAV_CHAPTER, user_node, chapter_node, time);
						
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}
	@Override
	public String bookmark_chapter(String chapter_id, String series_id,
			String user_id, int time) throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");
			if(chapter_id == null || chapter_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param chapter_id");

			aquireWriteLock(tx);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			
			Node chapter_node = chapterId_index.get(CHAPTER_ID, chapter_id).getSingle();
			if(chapter_node == null)
				throw new KahaniyaCustomException("Chapter doesnot exists with given id : "+chapter_id);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			if(user_node == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
			
			if(isRelationExistsBetween(USER_BOOKMARK_CHAPTER, user_node, chapter_node))
				deleteRelation(USER_BOOKMARK_CHAPTER, user_node, chapter_node);
			else
				createRelation(USER_BOOKMARK_CHAPTER, user_node, chapter_node, time);
						
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ bookmark_chapter()");
			System.out.println("Something went wrong, while bookmark a chapter from bookmark_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ bookmark_chapter()");
			System.out.println("Something went wrong, while bookmark a chapter from bookmark_chapter  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String rate_chapter(String chapter_id, String series_id, int rating,
			String user_id, int time) throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");
			if(chapter_id == null || chapter_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param chapter_id");

			aquireWriteLock(tx);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			
			Node chapter_node = chapterId_index.get(CHAPTER_ID, chapter_id).getSingle();
			if(chapter_node == null)
				throw new KahaniyaCustomException("Chapter doesnot exists with given id : "+chapter_id);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			if(user_node == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
			
			if(isRelationExistsBetween(USER_RATED_A_CHAPTER, user_node, chapter_node))
				deleteRelation(USER_RATED_A_CHAPTER, user_node, chapter_node);
			else
			{
				createRelation(USER_RATED_A_CHAPTER, user_node, chapter_node, time).setProperty(CHAPTER_RATING, rating);
			}			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String recored_chapter_view(String chapter_id, String series_id,
			String user_id, int time) throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param user_id");
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param series_id");
			if(chapter_id == null || chapter_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the param chapter_id");

			aquireWriteLock(tx);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Relationship> userViewedChapterRelIndex = graphDb.index().forRelationships(USER_VIEWED_CHAPTER_REL_INDEX);
			
			Node chapter_node = chapterId_index.get(CHAPTER_ID, chapter_id).getSingle();
			if(chapter_node == null)
				throw new KahaniyaCustomException("Chapter doesnot exists with given id : "+chapter_id);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			if(user_node == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
			
			Relationship rel = userViewedChapterRelIndex.get(USER_VIEWED_A_CHAPTER_ID, user_id+"_view_"+chapter_id).getSingle();
			if(rel != null)
			{
				rel.setProperty(CHAPTER_VIEWS, Integer.parseInt(rel.getProperty(CHAPTER_VIEWS).toString())+1);
			}
			else
			{
				rel = createRelation(USER_VIEWED_A_CHAPTER, user_node, chapter_node, time);
				rel.setProperty(CHAPTER_VIEWS, 1);
				userViewedChapterRelIndex.add(rel, USER_VIEWED_A_CHAPTER_ID, user_id+"_view_"+chapter_id);
			}			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String get_feed(String tileType, String feedType, String filter,
			int prev_cnt, int count, String s_user_id, String genre, String lang, String user_id) throws TException {
		if(feedType == null)
			feedType = "";
		if(tileType == null)
			tileType = "";
		if(filter == null)
			filter = "";
		if(user_id == null)
			user_id = "";
		if(s_user_id == null)
			s_user_id = "";
		if(genre == null)
			genre = "";
		if(lang == null)
			lang = "";
		
		if(feedType.equalsIgnoreCase("R"))
		{
			if(prev_cnt == 0)
				return get_recommended(filter, prev_cnt, count, s_user_id);
			else return get_recomended_by_series(filter, prev_cnt - 1, count, s_user_id);
		}
		else if(tileType.equalsIgnoreCase("A"))
			return get_authors(feedType, filter, prev_cnt, count, s_user_id);
		else if(tileType.equalsIgnoreCase("S"))
			return get_series(feedType, filter, prev_cnt, count, s_user_id, genre, lang, user_id);
		else if(tileType.equalsIgnoreCase("C"))
				return get_chapters(feedType, filter, prev_cnt, count, s_user_id, genre, lang, user_id);
		else return "";
	}
	
	private String get_recommended(String filter, int prev_cnt, int count, String user_id)
	{

		JSONArray jsonArray = new JSONArray();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> genre_lang_index = graphDb.index().forNodes(GENRE_LANG_INDEX);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			
			if(user_node == null)
				throw new KahaniyaCustomException("No user exists with given id : "+user_id);

			Iterator<Relationship> genres_itr = user_node.getRelationships(USER_INTERESTED_GENRE).iterator();					
			Iterator<Relationship> lang_itr = user_node.getRelationships(USER_INTERESTED_LANGUAGE).iterator();					
			
			LinkedList<Node> genres = new LinkedList<Node>();
			LinkedList<Node> langs = new LinkedList<Node>();
			
			LinkedList<Node> lang_genres = new LinkedList<Node>();

			while(genres_itr.hasNext())
			{
				genres.addLast(genres_itr.next().getEndNode());
			}
			while(lang_itr.hasNext())
			{
				langs.addLast(lang_itr.next().getEndNode());
			}
						
			for(Node gen : genres)
			{
				for(Node lang : langs)
				{
					Node n = genre_lang_index.get(GENRE_LANG_NAME, gen.getProperty(GENRE_NAME).toString() + " " + lang.getProperty(LANG_NAME).toString()).getSingle();
					if(n != null)
					{
						lang_genres.addLast(n);
					}
				}
			}
			
			LinkedList<Node> allChapterList = new LinkedList<Node>();
			
			for(Node n : lang_genres)
			{
				Iterator<Relationship> seriesRelItr = n.getRelationships(SERIES_BELONGS_TO_GENRE_LANGUAGE).iterator();
				while(seriesRelItr.hasNext())
				{
					Iterator<Relationship> chapterRelItr = seriesRelItr.next().getStartNode().getRelationships(CHAPTER_BELONGS_TO_SERIES).iterator();
					while(chapterRelItr.hasNext())
						allChapterList.addLast(chapterRelItr.next().getStartNode());
					
				}
				
			}
			
			
			Collections.sort(allChapterList, TrendingComparatorForChapterNodes);

			JSONObject jRecObj = new JSONObject();
			JSONArray jRecArray = new JSONArray();
			
			for(int i=0; i< 3; i++)
			{
				if(allChapterList.size() > i)
					jRecArray.put(getJSONForChapter(allChapterList.get(i), user_node));
			}
			if(jRecArray.length() > 0)
			{
				jRecObj.put("tp",0);
				jRecObj.put("data", jRecArray);

				jsonArray.put(jRecObj);
			} 

			for(Node n : langs)
			{
				JSONObject jobj = new JSONObject();
				JSONArray jarray = new JSONArray();
				
				Iterator<Relationship> seriesRelItr = n.getRelationships(SERIES_BELONGS_TO_LANGUAGE).iterator();
				LinkedList<Node> chapterList = new LinkedList<Node>();
				while(seriesRelItr.hasNext())
				{
					Iterator<Relationship> chapterRelItr = seriesRelItr.next().getStartNode().getRelationships(CHAPTER_BELONGS_TO_SERIES).iterator();
					while(chapterRelItr.hasNext())
					{
						chapterList.addLast(chapterRelItr.next().getStartNode());
					}
				}
				Collections.sort(chapterList, TimeCreatedComparatorForNodes);
				for(int i=0; i< 3; i++)
				{
					if(chapterList.size() > i)
						jarray.put(getJSONForChapter(chapterList.get(i), user_node));
				}
				if(jarray.length() > 0)
				{
					jobj.put("tp",1);
					jobj.put("ttl", n.getProperty(LANG_NAME));
					jobj.put("data", jarray);

					jsonArray.put(jobj);
				}
				
			}

			for(Node g : genres)
			{
				JSONObject jobj = new JSONObject();
				JSONArray jarray = new JSONArray();
				
				Iterator<Relationship> seriesRelItr = g.getRelationships(SERIES_BELONGS_TO_GENRE).iterator();
				LinkedList<Node> chapterList = new LinkedList<Node>();
				while(seriesRelItr.hasNext())
				{
					Iterator<Relationship> chapterRelItr = seriesRelItr.next().getStartNode().getRelationships(CHAPTER_BELONGS_TO_SERIES).iterator();
					while(chapterRelItr.hasNext())
					{
						chapterList.addLast(chapterRelItr.next().getStartNode());
					}
				}
				Collections.sort(chapterList, TimeCreatedComparatorForNodes);
				for(int i=0; i< 3; i++)
				{
					if(chapterList.size() > i)
						jarray.put(getJSONForChapter(chapterList.get(i), user_node));
				}
				if(jarray.length() > 0)
				{
					jobj.put("tp",2);
					jobj.put("ttl", g.getProperty(GENRE_NAME));
					jobj.put("data", jarray);

					jsonArray.put(jobj);
				}
				
			}
			
			
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_recommended()");
			System.out.println("Something went wrong, while returning recommended chapters from get_recommended  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_recommended()");
			System.out.println("Something went wrong, while returning recommended chapters from get_recommended  :"+ex.getMessage());
			ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		return jsonArray.toString();
	}

	private String get_recomended_by_series(String filter, int prev_cnt, int count, String user_id)
	{

		JSONArray jsonArray = new JSONArray();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> genre_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> lang_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			
			int c = 0;
			LinkedList<Node> outputSeriesNode = new LinkedList<Node>();
			
			if(user_node == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);

			ResourceIterator<Node> seriesNodesItr = seriesId_index.query(SERIES_ID, "*").iterator();
			
			LinkedList<Node> seriesList = new LinkedList<Node>();
			while(seriesNodesItr.hasNext())
				seriesList.addLast(seriesNodesItr.next());
			Collections.sort(seriesList, TrendingComparatorForSeriesNodes);				
			for(Node series : seriesList)
			{			
				if(c >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
				{
					break;
				}
				
				Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
				Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
				if(filter != null && !filter.equals(""))
				{
					JSONObject filterJSON = new JSONObject(filter);
					if(filterJSON.has("genre") && filterJSON.has("language"))
					{
						for(String genre: filterJSON.getString("genre").split(","))
						{
							Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
							if(genreNode != null && seriesGenreNode.equals(genreNode))
							{
								for(String lang: filterJSON.getString("language").split(","))
								{
									Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
									if(langNode != null && seriesLangNode.equals(langNode))
									{
										if(c < prev_cnt)
										{
											c++;
											continue;
										}
										c++;
										outputSeriesNode.addLast(series);			
										
									}
								}
							}
						}
					}
					else if(filterJSON.has("genre"))
					{
						for(String genre: filterJSON.getString("genre").split(","))
						{
							Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
							if(genreNode != null && seriesGenreNode.equals(genreNode))
							{
								if(c < prev_cnt)
								{
									c++;
									continue;
								}
								c++;
								outputSeriesNode.addLast(series);			
							}
						}
					}
					else if(filterJSON.has("language"))
					{
						for(String lang: filterJSON.getString("language").split(","))
						{
							Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
							if(langNode != null && seriesLangNode.equals(langNode))
							{
								if(c < prev_cnt)
								{
									c++;
									continue;
								}
								c++;
								outputSeriesNode.addLast(series);			
								
							}
						}
					}
					else // i.e., no need to apply filter
					{
						if(c < prev_cnt)
						{
							c++;
							continue;
						}
						c++;
						outputSeriesNode.addLast(series);			
						
					}
				}
				else // i.e., no need to apply filter
				{
					if(c < prev_cnt)
					{
						c++;
						continue;
					}
					c++;
					outputSeriesNode.addLast(series);			
					
				}
				
			}
			
			for(Node series : outputSeriesNode)
			{
				JSONObject jobj = new JSONObject();
				JSONArray jarray = new JSONArray();
				
				Iterator<Relationship> chaptersRelItr = series.getRelationships(CHAPTER_BELONGS_TO_SERIES).iterator();
				LinkedList<Node> chapterList = new LinkedList<Node>();
				while(chaptersRelItr.hasNext())
					chapterList.addLast(chaptersRelItr.next().getStartNode());
				Collections.sort(chapterList, TimeCreatedComparatorForNodes);
				for(int i=0; i< 3; i++)
				{
					if(chapterList.size() > i)
						jarray.put(getJSONForChapter(chapterList.get(i), user_node));
				}
				if(jarray.length() > 0)
				{
					jobj.put("ttl", series.getProperty(SERIES_TITLE));
					jobj.put("data", jarray);

					jsonArray.put(jobj);
				}
				
			}
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_series()");
			System.out.println("Something went wrong, while returning series from get_series  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_series()");
			System.out.println("Something went wrong, while returning series from get_series  :"+ex.getMessage());
			ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		return jsonArray.toString();
	}
	
	private String get_series(String feedType, String filter, int prev_cnt, int count, String s_user_id, String genre_name, String lang_name, String user_id)
	{

		JSONArray jsonArray = new JSONArray();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> genre_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> lang_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			Node s_user_node = userId_index.get(USER_ID, s_user_id).getSingle();
			
			int c = 0;
			LinkedList<Node> outputSeriesNode = new LinkedList<Node>();
			
			if(feedType.equalsIgnoreCase("SUB"))
			{
				if(s_user_node == null)
					throw new KahaniyaCustomException("User doesnot exists with given id : "+s_user_id);
				Iterator<Relationship> subscribedRelsItr = s_user_node.getRelationships(USER_SUBSCRIBED_TO_SERIES).iterator();
				LinkedList<Relationship> subscribedRelsList = new LinkedList<Relationship>();
				while(subscribedRelsItr.hasNext())
					subscribedRelsList.addLast(subscribedRelsItr.next());
				Collections.sort(subscribedRelsList, TimeCreatedComparatorForRelationships);				
				for(Relationship rel : subscribedRelsList)
				{				
					
					if(c >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					Node series = rel.getEndNode();
					Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
					Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											if(c < prev_cnt)
											{
												c++;
												continue;
											}
											c++;
											outputSeriesNode.addLast(series);			
											
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputSeriesNode.addLast(series);			
								}
							}
						}
						else if(filterJSON.has("language"))
						{
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputSeriesNode.addLast(series);			
									
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(c < prev_cnt)
							{
								c++;
								continue;
							}
							c++;
							outputSeriesNode.addLast(series);			
							
						}
					}
					else // i.e., no need to apply filter
					{
						if(c < prev_cnt)
						{
							c++;
							continue;
						}
						c++;
						outputSeriesNode.addLast(series);			
						
					}
					
				}
			}

			else if(feedType.equalsIgnoreCase("W"))
			{
				if(user_node == null)
					throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
				Iterator<Relationship> subscribedRelsItr = user_node.getRelationships(USER_STARTED_SERIES).iterator();
				LinkedList<Relationship> subscribedRelsList = new LinkedList<Relationship>();
				while(subscribedRelsItr.hasNext())
					subscribedRelsList.addLast(subscribedRelsItr.next());
				Collections.sort(subscribedRelsList, TimeCreatedComparatorForRelationships);				
				for(Relationship rel : subscribedRelsList)
				{				
					
					if(c >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					Node series = rel.getEndNode();
					
					if(!series.getProperty(SERIES_TYPE).toString().equals("2"))
						continue;
					
					Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
					Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											if(c < prev_cnt)
											{
												c++;
												continue;
											}
											c++;
											outputSeriesNode.addLast(series);			
											
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputSeriesNode.addLast(series);			
								}
							}
						}
						else if(filterJSON.has("language"))
						{
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputSeriesNode.addLast(series);			
									
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(c < prev_cnt)
							{
								c++;
								continue;
							}
							c++;
							outputSeriesNode.addLast(series);			
							
						}
					}
					else // i.e., no need to apply filter
					{
						if(c < prev_cnt)
						{
							c++;
							continue;
						}
						c++;
						outputSeriesNode.addLast(series);			
						
					}
					
				}
			}

			else if(feedType.equalsIgnoreCase("G"))
			{
				Node genreNode = genre_index.get(GENRE_NAME, genre_name.toLowerCase()).getSingle();
				if(genreNode == null)
					throw new KahaniyaCustomException("Invalid Genre name");
				Iterator<Relationship> seriesRelItr = genreNode.getRelationships(SERIES_BELONGS_TO_GENRE).iterator();
				LinkedList<Relationship> seriesRelList = new LinkedList<Relationship>();
				while(seriesRelItr.hasNext())
					seriesRelList.addLast(seriesRelItr.next());
				Collections.sort(seriesRelList, TimeCreatedComparatorForRelationships);				
				for(Relationship rel : seriesRelList)
				{				
					
					if(c >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					Node series = rel.getStartNode();
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);
						
						if(filterJSON.has("language"))
						{
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputSeriesNode.addLast(series);			
									
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(c < prev_cnt)
							{
								c++;
								continue;
							}
							c++;
							outputSeriesNode.addLast(series);			
							
						}
					}
					else // i.e., no need to apply filter
					{
						if(c < prev_cnt)
						{
							c++;
							continue;
						}
						c++;
						outputSeriesNode.addLast(series);			
						
					}
					
				}
			}

			else if(feedType.equalsIgnoreCase("LNG"))
			{
				Node langNode = lang_index.get(LANG_NAME, lang_name.toLowerCase()).getSingle();
				if(langNode == null)
					throw new KahaniyaCustomException("Invalid Language Name");
				Iterator<Relationship> seriesRelItr = langNode.getRelationships(SERIES_BELONGS_TO_LANGUAGE).iterator();
				LinkedList<Relationship> seriesRelList = new LinkedList<Relationship>();
				while(seriesRelItr.hasNext())
					seriesRelList.addLast(seriesRelItr.next());
				Collections.sort(seriesRelList, TimeCreatedComparatorForRelationships);				
				for(Relationship rel : seriesRelList)
				{				
					
					if(c >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					Node series = rel.getStartNode();
					Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);
						
						if(filterJSON.has("genre"))
						{
							for(String genr: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genr.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputSeriesNode.addLast(series);			
									
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(c < prev_cnt)
							{
								c++;
								continue;
							}
							c++;
							outputSeriesNode.addLast(series);			
							
						}
					}
					else // i.e., no need to apply filter
					{
						if(c < prev_cnt)
						{
							c++;
							continue;
						}
						c++;
						outputSeriesNode.addLast(series);			
						
					}
					
				}
			}

			else if(feedType.equalsIgnoreCase("D"))
			{
				ResourceIterator<Node> seriesNodesItr = seriesId_index.query(SERIES_ID, "*").iterator();
				LinkedList<Node> seriesList = new LinkedList<Node>();
				while(seriesNodesItr.hasNext())
					seriesList.addLast(seriesNodesItr.next());
				Collections.sort(seriesList, TrendingComparatorForSeriesNodes);				
				for(Node series : seriesList)
				{	
					if(c >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					if(!series.getProperty(SERIES_TYPE).toString().equals("2"))
						continue;
					
					if(series.getSingleRelationship(USER_STARTED_SERIES, Direction.INCOMING).getStartNode().equals(s_user_node))
						continue;
					
					Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
					Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											if(c < prev_cnt)
											{
												c++;
												continue;
											}
											c++;
											outputSeriesNode.addLast(series);			
											
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputSeriesNode.addLast(series);			
								}
							}
						}
						else if(filterJSON.has("language"))
						{
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputSeriesNode.addLast(series);			
									
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(c < prev_cnt)
							{
								c++;
								continue;
							}
							c++;
							outputSeriesNode.addLast(series);			
							
						}
					}
					else // i.e., no need to apply filter
					{
						if(c < prev_cnt)
						{
							c++;
							continue;
						}
						c++;
						outputSeriesNode.addLast(series);			
						
					}
					
				}
			}

			for(Node series : outputSeriesNode)
				jsonArray.put(getJSONForSeries(series, s_user_node));
			
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_series()");
			System.out.println("Something went wrong, while returning series from get_series  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_series()");
			System.out.println("Something went wrong, while returning series from get_series  :"+ex.getMessage());
			ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		return jsonArray.toString();
	}
	
	private String get_authors(String feedType, String filter, int prev_cnt, int count, String user_id)
	{
		JSONArray jsonArray = new JSONArray();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> genre_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> lang_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			if(user_id == null)
				user_id = "";
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			
			int c = 0;
			LinkedList<Node> outputUserNode = new LinkedList<Node>();

			if(feedType.equalsIgnoreCase("D"))
			{

				ResourceIterator<Node> seriesNodesItr = seriesId_index.query(SERIES_ID, "*").iterator();
				LinkedList<Node> seriesList = new LinkedList<Node>();
				while(seriesNodesItr.hasNext())
					seriesList.addLast(seriesNodesItr.next());
				Collections.sort(seriesList, TrendingComparatorForSeriesNodes);	

				for(Node series : seriesList)
				{				
					if(series.getSingleRelationship(USER_STARTED_SERIES, Direction.INCOMING).getStartNode().equals(user_node))
						continue;
					if(c >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
					Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											Node auth = series.getSingleRelationship(USER_STARTED_SERIES, Direction.INCOMING).getStartNode();
											if(auth.hasProperty(IS_DELETED) && auth.getProperty(IS_DELETED).toString().equals("1"))
												continue;
											if(isRelationExistsBetween(USER_FOLLOW_USER, user_node, auth))
												continue;
											if(c < prev_cnt)
											{
												c++;
												continue;
											}
											if(!outputUserNode.contains(auth))
											{

												c++;
												outputUserNode.addLast(auth);			
											}
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									Node auth = series.getSingleRelationship(USER_STARTED_SERIES, Direction.INCOMING).getStartNode();
									if(auth.hasProperty(IS_DELETED) && auth.getProperty(IS_DELETED).toString().equals("1"))
										continue;
									if(isRelationExistsBetween(USER_FOLLOW_USER, user_node, auth))
										continue;
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									if(!outputUserNode.contains(auth))
									{

										c++;
										outputUserNode.addLast(auth);			
									}			
								}
							}
						}
						else if(filterJSON.has("language"))
						{
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									Node auth = series.getSingleRelationship(USER_STARTED_SERIES, Direction.INCOMING).getStartNode();
									if(auth.hasProperty(IS_DELETED) && auth.getProperty(IS_DELETED).toString().equals("1"))
										continue;
									if(isRelationExistsBetween(USER_FOLLOW_USER, user_node, auth))
										continue;
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									if(!outputUserNode.contains(auth))
									{

										c++;
										outputUserNode.addLast(auth);			
									}
								}
							}
						}
						else // i.e., no need to apply filter
						{
							Node auth = series.getSingleRelationship(USER_STARTED_SERIES, Direction.INCOMING).getStartNode();
							if(auth.hasProperty(IS_DELETED) && auth.getProperty(IS_DELETED).toString().equals("1"))
								continue;
							if(isRelationExistsBetween(USER_FOLLOW_USER, user_node, auth))
								continue;
							if(c < prev_cnt)
							{
								c++;
								continue;
							}
							if(!outputUserNode.contains(auth))
							{

								c++;
								outputUserNode.addLast(auth);			
							}
						}
					}
					else // i.e., no need to apply filter
					{
						Node auth = series.getSingleRelationship(USER_STARTED_SERIES, Direction.INCOMING).getStartNode();
						if(auth.hasProperty(IS_DELETED) && auth.getProperty(IS_DELETED).toString().equals("1"))
							continue;
						if(isRelationExistsBetween(USER_FOLLOW_USER, user_node, auth))
							continue;
						if(c < prev_cnt)
						{
							c++;
							continue;
						}
						if(!outputUserNode.contains(auth))
						{

							c++;
							outputUserNode.addLast(auth);			
						}
					}
					
				}
			}

			for(Node user : outputUserNode)
				jsonArray.put(getJSONForUser(user, user_node));
			
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_authors()");
			System.out.println("Something went wrong, while returning authors from get_authors  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_authors()");
			System.out.println("Something went wrong, while returning authors from get_authors  :"+ex.getMessage());
			ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		return jsonArray.toString();
	}
	
	private String get_chapters(String feedType, String filter, int prev_cnt, int count, String s_user_id, String genre_name, String lang_name, String user_id)
	{

		JSONArray jsonArray = new JSONArray();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> genre_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> lang_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			
			Node s_user_node = userId_index.get(USER_ID, s_user_id).getSingle();
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();

			LinkedList<Node> outputChaptersNode = new LinkedList<Node>();

			if(feedType.equalsIgnoreCase("F"))
			{
				int c = 0;
				
				if(s_user_node == null)
					throw new KahaniyaCustomException("User doesnot exists with given id : "+s_user_id);
				Iterator<Relationship> favCHaptersRelsItr = s_user_node.getRelationships(USER_FAV_CHAPTER).iterator();
				LinkedList<Relationship> favChaptersRelsList = new LinkedList<Relationship>();
				while(favCHaptersRelsItr.hasNext())
					favChaptersRelsList.addLast(favCHaptersRelsItr.next());
				Collections.sort(favChaptersRelsList, TimeCreatedComparatorForRelationships);
								
				for(Relationship rel : favChaptersRelsList)
				{
					if(c >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					Node chapter = rel.getEndNode();
					
					if(filter != null && !filter.equals(""))
					{
						
						JSONObject filterJSON = new JSONObject(filter);	
						
						if(filterJSON.has("price") && !filterJSON.getString("price").equals(chapter.getProperty(CHAPTER_FREE_OR_PAID).toString()))
							continue;
						
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											if(c < prev_cnt)
											{
												c++;
												continue;
											}
											c++;
											outputChaptersNode.addLast(chapter);
	
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else if(filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(c < prev_cnt)
							{
								c++;
								continue;
							}
							c++;
							outputChaptersNode.addLast(chapter);
						}
					}
					else // i.e., no need to apply filter
					{
						if(c < prev_cnt)
						{
							c++;
							continue;
						}
						c++;
						outputChaptersNode.addLast(chapter);
					}
					
				}
			}
			else if(feedType.equalsIgnoreCase("B"))
			{
				int c = 0;
				
				if(s_user_node == null)
					throw new KahaniyaCustomException("User doesnot exists with given id : "+s_user_id);
				Iterator<Relationship> favCHaptersRelsItr = s_user_node.getRelationships(USER_BOOKMARK_CHAPTER).iterator();
				LinkedList<Relationship> favChaptersRelsList = new LinkedList<Relationship>();
				while(favCHaptersRelsItr.hasNext())
					favChaptersRelsList.addLast(favCHaptersRelsItr.next());
				Collections.sort(favChaptersRelsList, TimeCreatedComparatorForRelationships);
								
				for(Relationship rel : favChaptersRelsList)
				{
					if(c >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					Node chapter = rel.getEndNode();
					
					if(filter != null && !filter.equals(""))
					{
						
						JSONObject filterJSON = new JSONObject(filter);	
						
						if(filterJSON.has("price") && !filterJSON.getString("price").equals(chapter.getProperty(CHAPTER_FREE_OR_PAID).toString()))
							continue;
						
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											if(c < prev_cnt)
											{
												c++;
												continue;
											}
											c++;
											outputChaptersNode.addLast(chapter);
	
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else if(filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(c < prev_cnt)
									{
										c++;
										continue;
									}
									c++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(c < prev_cnt)
							{
								c++;
								continue;
							}
							c++;
							outputChaptersNode.addLast(chapter);
						}
					}
					else // i.e., no need to apply filter
					{
						if(c < prev_cnt)
						{
							c++;
							continue;
						}
						c++;
						outputChaptersNode.addLast(chapter);
					}
					
				}
			}
			
			else if(feedType.equalsIgnoreCase("T"))
			{
				Index<Node> chapter_id_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
				ResourceIterator<Node> allChapters = chapter_id_index.query(CHAPTER_ID, "*").iterator();
				LinkedList<Node> allChaptersList = new LinkedList<Node>();
				while(allChapters.hasNext())
					allChaptersList.addLast(allChapters.next());
				Collections.sort(allChaptersList, TrendingComparatorForChapterNodes);
					
				int i = 0;
				for(Node chapter : allChaptersList)
				{
					if(i >= 10) // break the loop, if we got enough / required nodes to return
						break;
					
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);	

						if(filterJSON.has("price") && !filterJSON.getString("price").equals(chapter.getProperty(CHAPTER_FREE_OR_PAID).toString()))
							continue;
						
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											if(i < prev_cnt)
											{
												i++;
												continue;
											}
											
											i++;
											outputChaptersNode.addLast(chapter);
	
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else if(filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(i < prev_cnt)
							{
								i++;
								continue;
							}
							
							i++;
							outputChaptersNode.addLast(chapter);
						}
					}
					else // i.e., no need to apply filter
					{
						if(i < prev_cnt)
						{
							i++;
							continue;
						}
						
						i++;
						outputChaptersNode.addLast(chapter);
					}
					
				}
			}

			
			else if(feedType.equalsIgnoreCase("G"))
			{
				Node genreNode = genre_index.get(GENRE_NAME, genre_name.toLowerCase()).getSingle();
				if(genreNode == null)
					throw new KahaniyaCustomException("Invalid Genre name");
				Iterator<Relationship> seriesRelItr = genreNode.getRelationships(SERIES_BELONGS_TO_GENRE).iterator();
				LinkedList<Node> chaptersList = new LinkedList<Node>();
				while(seriesRelItr.hasNext())
				{
					Node t = seriesRelItr.next().getStartNode();
					Iterator<Relationship> chaptersRelItr = t.getRelationships(CHAPTER_BELONGS_TO_SERIES).iterator();
					while(chaptersRelItr.hasNext())
						chaptersList.addLast(chaptersRelItr.next().getStartNode());
				}
				Collections.sort(chaptersList, TimeCreatedComparatorForNodes);				
				int i = 0;
				for(Node chapter : chaptersList)
				{
					if(i >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);	
						if(filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(i < prev_cnt)
							{
								i++;
								continue;
							}
							
							i++;
							outputChaptersNode.addLast(chapter);
						}
					}
					else // i.e., no need to apply filter
					{
						if(i < prev_cnt)
						{
							i++;
							continue;
						}
						
						i++;
						outputChaptersNode.addLast(chapter);
					}
					
				}
			}

			
			else if(feedType.equalsIgnoreCase("LNG"))
			{
				Node langNode = lang_index.get(LANG_NAME, lang_name.toLowerCase()).getSingle();
				if(langNode == null)
					throw new KahaniyaCustomException("Invalid Language name");
				Iterator<Relationship> seriesRelItr = langNode.getRelationships(SERIES_BELONGS_TO_LANGUAGE).iterator();
				LinkedList<Node> chaptersList = new LinkedList<Node>();
				while(seriesRelItr.hasNext())
				{
					Node t = seriesRelItr.next().getStartNode();
					Iterator<Relationship> chaptersRelItr = t.getRelationships(CHAPTER_BELONGS_TO_SERIES).iterator();
					while(chaptersRelItr.hasNext())
						chaptersList.addLast(chaptersRelItr.next().getStartNode());
				}
				Collections.sort(chaptersList, TimeCreatedComparatorForNodes);				
				int i = 0;
				for(Node chapter : chaptersList)
				{
					if(i >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);	
						
						if(filterJSON.has("price") && !filterJSON.getString("price").equals(chapter.getProperty(CHAPTER_FREE_OR_PAID).toString()))
							continue;
						
						if(filterJSON.has("genre"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(langNode != null && seriesGenreNode.equals(genreNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(i < prev_cnt)
							{
								i++;
								continue;
							}
							
							i++;
							outputChaptersNode.addLast(chapter);
						}
					}
					else // i.e., no need to apply filter
					{
						if(i < prev_cnt)
						{
							i++;
							continue;
						}
						
						i++;
						outputChaptersNode.addLast(chapter);
					}
					
				}
			}
			
			else if(feedType.equalsIgnoreCase("D"))
			{

				Index<Node> chapter_id_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
				ResourceIterator<Node> allChapters = chapter_id_index.query(CHAPTER_ID, "*").iterator();
				LinkedList<Node> allChaptersList = new LinkedList<Node>();
				while(allChapters.hasNext())
					allChaptersList.addLast(allChapters.next());
				Collections.sort(allChaptersList, TrendingComparatorForChapterNodes);
					
				int i = 0;
				for(Node chapter : allChaptersList)
				{
					if(chapter.getSingleRelationship(USER_WRITTEN_A_CHAPTER, Direction.INCOMING).getStartNode().equals(s_user_node))
						continue;
					if(i >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);	
						
						if(filterJSON.has("price") && !filterJSON.getString("price").equals(chapter.getProperty(CHAPTER_FREE_OR_PAID).toString()))
							continue;
						
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{

							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											if(i < prev_cnt)
											{
												i++;
												continue;
											}
											
											i++;
											outputChaptersNode.addLast(chapter);
	
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{

							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else if(filterJSON.has("language"))
						{

							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(i < prev_cnt)
							{
								i++;
								continue;
							}
							
							i++;
							outputChaptersNode.addLast(chapter);
						}
					}
					else // i.e., no need to apply filter
					{
						if(i < prev_cnt)
						{
							i++;
							continue;
						}
						
						i++;
						outputChaptersNode.addLast(chapter);
					}
					
				}
			}
			
			else if(feedType.equalsIgnoreCase("L"))
			{
				Index<Node> chapter_id_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
				ResourceIterator<Node> allChapters = chapter_id_index.query(CHAPTER_ID, "*").iterator();
				LinkedList<Node> allChaptersList = new LinkedList<Node>();
				while(allChapters.hasNext())
					allChaptersList.addLast(allChapters.next());
				Collections.sort(allChaptersList, TimeCreatedComparatorForNodes);
					
				int i = 0;
				for(Node chapter : allChaptersList)
				{
					if(i >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);	
						
						if(filterJSON.has("price") && !filterJSON.getString("price").equals(chapter.getProperty(CHAPTER_FREE_OR_PAID).toString()))
							continue;
						
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											if(i < prev_cnt)
											{
												i++;
												continue;
											}
											
											i++;
											outputChaptersNode.addLast(chapter);
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else if(filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(i < prev_cnt)
							{
								i++;
								continue;
							}
							
							i++;
							outputChaptersNode.addLast(chapter);
						}
					}
					else // i.e., no need to apply filter
					{
						if(i < prev_cnt)
						{
							i++;
							continue;
						}
						
						i++;
						outputChaptersNode.addLast(chapter);
					}
					
				}
			}

			else if(feedType.equalsIgnoreCase("H"))
			{
				int i = 0;
				if(s_user_node == null)
					throw new KahaniyaCustomException("User doesnot exists with given id : "+s_user_id);
				Iterator<Relationship> favCHaptersRelsItr = s_user_node.getRelationships(USER_VIEWED_A_CHAPTER).iterator();
				LinkedList<Relationship> favChaptersRelsList = new LinkedList<Relationship>();
				while(favCHaptersRelsItr.hasNext())
					favChaptersRelsList.addLast(favCHaptersRelsItr.next());
				Collections.sort(favChaptersRelsList, TimeCreatedComparatorForRelationships);
								
				for(Relationship rel : favChaptersRelsList)
				{
					if(i >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					Node chapter = rel.getEndNode();
					
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);	
						
						if(filterJSON.has("price") && !filterJSON.getString("price").equals(chapter.getProperty(CHAPTER_FREE_OR_PAID).toString()))
							continue;
						
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											if(i < prev_cnt)
											{
												i++;
												continue;
											}
											
											i++;
											outputChaptersNode.addLast(chapter);
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else if(filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(i < prev_cnt)
							{
								i++;
								continue;
							}
							
							i++;
							outputChaptersNode.addLast(chapter);
						}
					}
					else // i.e., no need to apply filter
					{
						if(i < prev_cnt)
						{
							i++;
							continue;
						}
						
						i++;
						outputChaptersNode.addLast(chapter);
					}
					
				}
			}

			else if(feedType.equalsIgnoreCase("W"))
			{
				int i = 0;
				if(user_node == null)
					throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
				Iterator<Relationship> favCHaptersRelsItr = user_node.getRelationships(USER_WRITTEN_A_CHAPTER).iterator();
				LinkedList<Relationship> favChaptersRelsList = new LinkedList<Relationship>();
				while(favCHaptersRelsItr.hasNext())
					favChaptersRelsList.addLast(favCHaptersRelsItr.next());
				Collections.sort(favChaptersRelsList, TimeCreatedComparatorForRelationships);
								
				for(Relationship rel : favChaptersRelsList)
				{
					if(i >= prev_cnt + count) // break the loop, if we got enough / required nodes to return
						break;
					
					Node chapter = rel.getEndNode();
					
					if(filter != null && !filter.equals(""))
					{
						JSONObject filterJSON = new JSONObject(filter);	
						
						if(filterJSON.has("price") && !filterJSON.getString("price").equals(chapter.getProperty(CHAPTER_FREE_OR_PAID).toString()))
							continue;
						
						if(filterJSON.has("genre") && filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									for(String lang: filterJSON.getString("language").split(","))
									{
										Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
										if(langNode != null && seriesLangNode.equals(langNode))
										{
											if(i < prev_cnt)
											{
												i++;
												continue;
											}
											
											i++;
											outputChaptersNode.addLast(chapter);
										}
									}
								}
							}
						}
						else if(filterJSON.has("genre"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesGenreNode = series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode();
							for(String genre: filterJSON.getString("genre").split(","))
							{
								Node genreNode = genre_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
								if(genreNode != null && seriesGenreNode.equals(genreNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else if(filterJSON.has("language"))
						{
							Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
							Node seriesLangNode = series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode();
							for(String lang: filterJSON.getString("language").split(","))
							{
								Node langNode = lang_index.get(LANG_NAME, lang.toLowerCase()).getSingle();
								if(langNode != null && seriesLangNode.equals(langNode))
								{
									if(i < prev_cnt)
									{
										i++;
										continue;
									}
									
									i++;
									outputChaptersNode.addLast(chapter);
								}
							}
						}
						else // i.e., no need to apply filter
						{
							if(i < prev_cnt)
							{
								i++;
								continue;
							}
							
							i++;
							outputChaptersNode.addLast(chapter);
						}
					}
					else // i.e., no need to apply filter
					{
						if(i < prev_cnt)
						{
							i++;
							continue;
						}
						
						i++;
						outputChaptersNode.addLast(chapter);
					}
					
				}
			}

			for(Node chapter : outputChaptersNode)
				jsonArray.put(getJSONForChapter(chapter, s_user_node));
			
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_chapters()");
			System.out.println("Something went wrong, while returning chapters from get_chapters  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_chapters()");
			System.out.println("Something went wrong, while returning chapters from get_chapters  :"+ex.getMessage());
			ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		return jsonArray.toString();
	}
	
	private JSONObject getJSONForChapter(Node chapter, Node req_user)
	{
		JSONObject obj = new JSONObject();
		obj.put("P_Author_FullName",chapter.getSingleRelationship(USER_WRITTEN_A_CHAPTER, Direction.INCOMING).getStartNode().getProperty(FULL_NAME).toString());
		obj.put("P_Author",chapter.getSingleRelationship(USER_WRITTEN_A_CHAPTER, Direction.INCOMING).getStartNode().getProperty(USER_ID).toString());
		obj.put("P_Title_ID",chapter.getProperty(CHAPTER_TITLE_ID).toString());
		obj.put("P_Title",chapter.getProperty(CHAPTER_TITLE).toString());
		obj.put("P_Id",chapter.getProperty(CHAPTER_ID).toString());
		
		Node series = chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode();
		
		if(!series.getProperty(SERIES_TYPE).toString().equals("2"))
			obj.put("P_Feature_Image",series.getProperty(SERIES_FEAT_IMG));
		else
			obj.put("P_Feature_Image",chapter.getProperty(CHAPTER_FEAT_IMAGE));
		
		JSONObject seriesJSON = new JSONObject();
		seriesJSON.put("Series_Id", series.getProperty(SERIES_ID).toString());
		seriesJSON.put("Series_Ttl", series.getProperty(SERIES_TITLE).toString());
		seriesJSON.put("Series_Tid", series.getProperty(SERIES_TITLE_ID).toString());
		seriesJSON.put("Series_Typ", series.getProperty(SERIES_TYPE));
		
		obj.put("Series_Info", seriesJSON);
		obj.put("P_Genre",series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode().getProperty(GENRE_NAME).toString());
		obj.put("P_Lang",series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode().getProperty(LANG_NAME).toString());
		obj.put("P_TimeCreated",chapter.getProperty(TIME_CREATED).toString());
		obj.put("P_Num_Views",chapter.getDegree(USER_VIEWED_A_CHAPTER, Direction.INCOMING));
		obj.put("P_Num_Fvrts",chapter.getDegree(USER_FAV_CHAPTER, Direction.INCOMING));
		obj.put("Is_Neo4j",true);
		
		int tot_rating = 0;
		Iterator<Relationship> ratingRelItr = chapter.getRelationships(Direction.INCOMING, USER_RATED_A_CHAPTER).iterator();
		while(ratingRelItr.hasNext())
			tot_rating = tot_rating + Integer.parseInt(ratingRelItr.next().getProperty(CHAPTER_RATING).toString());
		if(chapter.getDegree(USER_RATED_A_CHAPTER, Direction.INCOMING) == 0)
			obj.put("P_Rating",tot_rating);
		else
			obj.put("P_Rating",tot_rating/chapter.getDegree(USER_RATED_A_CHAPTER, Direction.INCOMING));

		if(req_user == null)
			obj.put("P_Is_Fvrt",0);
		else
		{
			obj.put("P_Is_Fvrt",0);			
			Iterator<Relationship> favRels = req_user.getRelationships(USER_FAV_CHAPTER, Direction.OUTGOING).iterator();
			while(favRels.hasNext())
			{
				if(favRels.next().getEndNode().equals(chapter))
				{
					obj.put("P_Is_Fvrt",1);
					break;
				}
			}
		}
		if(req_user == null)
			obj.put("P_Is_Bmrk",0);
		else
		{
			obj.put("P_Is_Bmrk",0);			
			Iterator<Relationship> bmRels = req_user.getRelationships(USER_BOOKMARK_CHAPTER, Direction.OUTGOING).iterator();
			while(bmRels.hasNext())
			{
				if(bmRels.next().getEndNode().equals(chapter))
				{
					obj.put("P_Is_Bmrk",1);
					break;
				}
			}
		}
		
		return obj;
	}

	private JSONObject getJSONForUser(Node user, Node req_user)
	{
		JSONObject obj = new JSONObject();
		obj.put("FullName",user.getProperty(FULL_NAME).toString());
		obj.put("UserId",user.getProperty(USER_ID).toString());
		obj.put("Mobile_Dial_Code",user.getProperty(MOBILE_DIAL_CODE).toString());
		obj.put("U_Num_Following",user.getDegree(USER_FOLLOW_USER, Direction.OUTGOING));
		obj.put("U_Num_Subscribers",user.getDegree(USER_FOLLOW_USER, Direction.INCOMING));
		if(req_user == null)
			obj.put("U_Is_Following",0);
		else
		{

			obj.put("U_Is_Following",0);
			
			Iterator<Relationship> followingRels = req_user.getRelationships(USER_FOLLOW_USER, Direction.OUTGOING).iterator();
			while(followingRels.hasNext())
			{
				if(followingRels.next().getEndNode().equals(user))
				{
					obj.put("U_Is_Following",1);
					break;
				}
			}
		}
		obj.put("Is_Neo4j",true);
		return obj;
	}
	
	private JSONObject getJSONForComment(Node comment)
	{
		JSONObject obj = new JSONObject();
		obj.put("Cm_By_Fullname",comment.getSingleRelationship(USER_WRITTEN_A_COMMENT, Direction.INCOMING).getStartNode().getProperty(FULL_NAME).toString());
		obj.put("Cm_By_UserId",comment.getSingleRelationship(USER_WRITTEN_A_COMMENT, Direction.INCOMING).getStartNode().getProperty(USER_ID).toString());
		obj.put("Cm_ID",comment.getProperty(COMMENT_ID).toString());
		obj.put("Cm_CID",comment.getSingleRelationship(COMMENT_WRITTEN_ON_CHAPTER, Direction.OUTGOING).getEndNode().getProperty(CHAPTER_ID).toString());
		obj.put("Cm_Cntnt", comment.getProperty(COMMENT_CONTENT).toString());
		obj.put("Cm_TimeCreated",comment.getProperty(TIME_CREATED).toString());
		obj.put("Cm_Rply_Cnt",comment.getDegree(REPLY_COMMENT_WRITTEN_ON_COMMENT, Direction.INCOMING));
		obj.put("Is_Neo4j",true);
		return obj;
	}
	
	private JSONObject getJSONForSeries(Node series, Node user)
	{
		JSONObject obj = new JSONObject();		
		obj.put("P_Author_FullName",series.getSingleRelationship(USER_STARTED_SERIES, Direction.INCOMING).getStartNode().getProperty(FULL_NAME).toString());
		obj.put("P_Author",series.getSingleRelationship(USER_STARTED_SERIES, Direction.INCOMING).getStartNode().getProperty(USER_ID).toString());
		obj.put("P_Title_ID",series.getProperty(SERIES_TITLE_ID).toString());
		obj.put("P_Title",series.getProperty(SERIES_TITLE).toString());
		obj.put("P_Feature_Image",series.getProperty(SERIES_FEAT_IMG).toString());
		obj.put("P_Id",series.getProperty(SERIES_ID).toString());
		obj.put("P_Genre",series.getSingleRelationship(SERIES_BELONGS_TO_GENRE, Direction.OUTGOING).getEndNode().getProperty(GENRE_NAME).toString());
		obj.put("P_Lang",series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode().getProperty(LANG_NAME).toString());
		obj.put("P_TimeCreated",series.getProperty(TIME_CREATED).toString());
		obj.put("P_Num_Views",0);
		obj.put("P_Num_Fvrts",0);
		obj.put("P_Rating",0);
		obj.put("P_Type", series.getProperty(SERIES_TYPE).toString());
		obj.put("P_Num_Chapters", series.getDegree(CHAPTER_BELONGS_TO_SERIES));
		obj.put("P_Num_Subscribers", series.getDegree(USER_SUBSCRIBED_TO_SERIES));
		if(user == null)
			obj.put("Is_Subscribe",0);
		else
		{
			obj.put("Is_Subscribe",0);			
			Iterator<Relationship> followingRels = user.getRelationships(USER_SUBSCRIBED_TO_SERIES, Direction.OUTGOING).iterator();
			while(followingRels.hasNext())
			{
				if(followingRels.next().getEndNode().equals(series))
				{
					obj.put("Is_Subscribe",1);
					break;
				}
			}
		}

		obj.put("Is_Neo4j",true);
		return obj;
	}
	
	private JSONObject getShortJSONForSeries(Node series, Node user)
	{
		JSONObject obj = new JSONObject();		
		obj.put("P_Id",series.getProperty(SERIES_ID).toString());
		obj.put("P_Title_ID",series.getProperty(SERIES_TITLE_ID).toString());
		obj.put("P_Title",series.getProperty(SERIES_TITLE).toString());
		obj.put("P_Feature_Image",series.getProperty(SERIES_FEAT_IMG).toString());
		obj.put("P_Num_Subscribers", series.getDegree(USER_SUBSCRIBED_TO_SERIES));
		if(user == null)
			obj.put("Is_Subscribe",0);
		else
		{
			obj.put("Is_Subscribe",0);			
			Iterator<Relationship> followingRels = user.getRelationships(USER_SUBSCRIBED_TO_SERIES, Direction.OUTGOING).iterator();
			while(followingRels.hasNext())
			{
				if(followingRels.next().getEndNode().equals(series))
				{
					obj.put("Is_Subscribe",1);
					break;
				}
			}
		}
		
		obj.put("P_Lang", series.getSingleRelationship(SERIES_BELONGS_TO_LANGUAGE, Direction.OUTGOING).getEndNode().getProperty(LANG_NAME));

		obj.put("Is_Neo4j",true);
		return obj;
	}

	@Override
	public String create_or_edit_comment(String chapter_id, String comment_id,
			String content, String parent_cmnt_id, String user_id, int time, int is_edit)
			throws TException {
		if(is_edit == 0)
			return create_comment(chapter_id, comment_id, content, parent_cmnt_id, user_id, time);
		else if(is_edit == 1)
			return edit_comment(chapter_id, comment_id, content, parent_cmnt_id, user_id, time);
		else return "false";		
	}
		
	public String create_comment(String chapter_id, String comment_id,
			String content, String parent_cmnt_id, String user_id, int time)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{

			if(comment_id == null || comment_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty comment id");
			if(chapter_id == null || chapter_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty chapter id");
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty user id");
			if(parent_cmnt_id == null)
				parent_cmnt_id = "";
			if(content == null)
				content = "";
			
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> commentId_index = graphDb.index().forNodes(COMMENT_ID_INDEX);
			
			Node userNode = userId_index.get(USER_ID,user_id).getSingle();
			if(userNode == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);

			Node chapter_node = chapterId_index.get(CHAPTER_ID,chapter_id).getSingle();
			if(chapter_node == null)
				throw new KahaniyaCustomException("Chapter does not exists with given id : "+chapter_id);
			
			Node parentCommentNode = null;
			if(parent_cmnt_id != null && !parent_cmnt_id.equals(""))
			parentCommentNode = commentId_index.get(COMMENT_ID,parent_cmnt_id).getSingle();
			
			if(commentId_index.get(COMMENT_ID,comment_id).getSingle() != null)
				throw new KahaniyaCustomException("Comment already exists with given id : "+comment_id);
			
			Node comment_node = Comment(comment_id, content, time);  // Creating a new comment node
			if(comment_node == null)
				throw new KahaniyaCustomException("Something went wrong, while creating comment ");

			//create relationship with user
			createRelation(USER_WRITTEN_A_COMMENT, userNode, comment_node, time);
			
			//create relationships with Chapter
			createRelation(COMMENT_WRITTEN_ON_CHAPTER, comment_node, chapter_node, time);

			//create relationships with parent commment
			if(parentCommentNode != null)
				createRelation(REPLY_COMMENT_WRITTEN_ON_COMMENT, comment_node, parentCommentNode, time);
			
			//Indexing newly created comment node
			commentId_index.add(comment_node, COMMENT_ID, comment_id);
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_comment()");
			System.out.println("Something went wrong, while creating comment from create_comment  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ create_comment()");
			System.out.println("Something went wrong, while creating comment from create_comment  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;

	}
	
	public String edit_comment(String chapter_id, String comment_id,
			String content, String parent_cmnt_id, String user_id, int time)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			if(comment_id == null || comment_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty comment id");
			if(chapter_id == null || chapter_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty chapter id");
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty user id");
			if(parent_cmnt_id == null)
				parent_cmnt_id = "";
			if(content == null)
				content = "";
			aquireWriteLock(tx);
			Index<Node> commentId_index = graphDb.index().forNodes(COMMENT_ID_INDEX);
			
			Node commentNode = commentId_index.get(COMMENT_ID,comment_id).getSingle();
			if(commentNode == null)
				throw new KahaniyaCustomException("Comment does not exists with given id : "+comment_id);
			
			commentNode.setProperty(COMMENT_CONTENT, content);
						
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_comment()");
			System.out.println("Something went wrong, while editing comment from edit_comment  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ edit_comment()");
			System.out.println("Something went wrong, while editing comment from edit_comment  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String delete_comment(String comment_id) throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{

			if(comment_id == null || comment_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty comment id");
			aquireWriteLock(tx);
			Index<Node> commentId_index = graphDb.index().forNodes(COMMENT_ID_INDEX);
			
			Node commentNode = commentId_index.get(COMMENT_ID,comment_id).getSingle();
			if(commentNode == null)
				throw new KahaniyaCustomException("Comment does not exists with given id : "+comment_id);
			
			commentId_index.remove(commentNode);
			Iterator<Relationship> relItr = commentNode.getRelationships().iterator();
			while(relItr.hasNext())
				relItr.next().delete();
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_comment()");
			System.out.println("Something went wrong, while deleting comment from delete_comment  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_comment()");
			System.out.println("Something went wrong, while deleting comment from delete_comment  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}
	
	@Override
	public String get_comments(String chapter_id, int prev_cnt, int count) throws TException {

		JSONArray jsonArray = new JSONArray();		
		try(Transaction tx = graphDb.beginTx())
		{
			if(chapter_id == null || chapter_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty chapter id");
			aquireWriteLock(tx);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			
			Node chapter_node = chapterId_index.get(CHAPTER_ID,chapter_id).getSingle();
			if(chapter_node == null)
				throw new KahaniyaCustomException("Chapter does not exists with given id : "+chapter_id);
			
			Iterator<Relationship> cmntsRelItr = chapter_node.getRelationships(COMMENT_WRITTEN_ON_CHAPTER).iterator();
			LinkedList<Relationship> cmntsRelList = new LinkedList<Relationship>();
			while(cmntsRelItr.hasNext())
				cmntsRelList.addLast(cmntsRelItr.next());
			Collections.sort(cmntsRelList, TimeCreatedComparatorForRelationships);
			
			int c = 0;
			for(Relationship rel : cmntsRelList)
			{
				if(c < prev_cnt)
					continue;
				else if ( c > prev_cnt+count)
					break;
				else
					jsonArray.put(getJSONForComment(rel.getStartNode()));
			}
			
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_comments()");
			System.out.println("Something went wrong, while returning comments from get_comments  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_comments()");
			System.out.println("Something went wrong, while returning comments from get_comments  :"+ex.getMessage());
			ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		return jsonArray.toString();
	}

	@Override
	public String get_all_items(String item_type, int prev_cnt, int count)
			throws TException {

		JSONArray jsonArray = new JSONArray();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			
			if(item_type == null || item_type.equals(""))
				throw new KahaniyaCustomException("Invalid item type");
			if(item_type.equalsIgnoreCase("C"))
			{
				int c = 0;

				Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
				ResourceIterator<Node> chaptersItr = chapterId_index.query(CHAPTER_ID, "*").iterator();
				LinkedList<Node> chaptersList = new LinkedList<Node>();
				while(chaptersItr.hasNext())
				{
					if(c < prev_cnt)
					{
						c++;
						continue;
					}
					else if ( c > prev_cnt+count)
						break;
					else
						chaptersList.addLast(chaptersItr.next());
					c++;
				}
				
				Collections.sort(chaptersList, TimeCreatedComparatorForNodes);
			
				for(Node chapter : chaptersList)
					jsonArray.put(getJSONForChapter(chapter, null));
			}
			else if(item_type.equalsIgnoreCase("S"))
			{
				int c = 0;

				Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
				ResourceIterator<Node> seriesItr = seriesId_index.query(SERIES_ID, "*").iterator();
				LinkedList<Node> seriesList = new LinkedList<Node>();
				while(seriesItr.hasNext())
				{
					if(c < prev_cnt)
					{
						c++;
						continue;
					}
					else if ( c > prev_cnt+count)
						break;
					else
						seriesList.addLast(seriesItr.next());
					c++;
				}
				
				Collections.sort(seriesList, TimeCreatedComparatorForNodes);
			
				for(Node series : seriesList)
					jsonArray.put(getJSONForSeries(series, null));
			}
			else if(item_type.equalsIgnoreCase("A") || item_type.equalsIgnoreCase("U"))
			{
				int c = 0;

				Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
				ResourceIterator<Node> userItr = userId_index.query(USER_ID, "*").iterator();
				LinkedList<Node> userList = new LinkedList<Node>();
				while(userItr.hasNext())
				{
					if(c < prev_cnt)
					{
						c++;
						continue;
					}
					else if ( c > prev_cnt+count)
						break;
					else
						userList.addLast(userItr.next());
					c++;
				}
				
				Collections.sort(userList, TimeCreatedComparatorForNodes);
			
				for(Node user : userList)
					jsonArray.put(getJSONForUser(user, null));
			}
			else if(item_type.equalsIgnoreCase("R"))
			{
				int c = 0;

				Index<Node> reviewId_index = graphDb.index().forNodes(REVIEW_ID_INDEX);
				ResourceIterator<Node> reviewItr = reviewId_index.query(REVIEW_ID, "*").iterator();
				LinkedList<Node> reviewList = new LinkedList<Node>();
				while(reviewItr.hasNext())
				{
					if(c < prev_cnt)
					{
						c++;
						continue;
					}
					else if ( c > prev_cnt+count)
						break;
					else
						reviewList.addLast(reviewItr.next());
					c++;
				}
				
				Collections.sort(reviewList, TimeCreatedComparatorForNodes);
			
				for(Node review : reviewList)
					jsonArray.put(getJSONForReview(review));
			}
			
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_all_items()");
			System.out.println("Something went wrong, while returning items from get_all_items  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_all_items()");
			System.out.println("Something went wrong, while returning items from get_all_items  :"+ex.getMessage());
			ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		return jsonArray.toString();		
	}

	@Override
	public String get_item_details(String item_type, String item_id)
			throws TException {

		JSONObject jsonObject = new JSONObject();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);

			if(item_type == null || item_type.equals(""))
				throw new KahaniyaCustomException("Invalid item type");
			if(item_id == null || item_id.equals(""))
				throw new KahaniyaCustomException("Invalid item id");
			
			if(item_type.equalsIgnoreCase("C"))
			{
				Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
				Node chapter = chapterId_index.get(CHAPTER_ID, item_id).getSingle();
				if(chapter == null)
					throw new KahaniyaCustomException("Invalid chapter id");
				jsonObject = getJSONForChapter(chapter, null);
			}
			else if(item_type.equalsIgnoreCase("S"))
			{
				Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
				Node series = seriesId_index.get(SERIES_ID, item_id).getSingle();
				if(series == null)
					throw new KahaniyaCustomException("Invalid series id");
				jsonObject = getJSONForSeries(series, null);
			}
			else if(item_type.equalsIgnoreCase("A") || item_type.equalsIgnoreCase("U"))
			{
				Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
				Node user = userId_index.get(USER_ID, item_id).getSingle();
				if(user == null)
					throw new KahaniyaCustomException("Invalid user id");
				jsonObject = getJSONForUser(user, null);
			}
			else if(item_type.equalsIgnoreCase("R"))
			{
				Index<Node> reviewId_index = graphDb.index().forNodes(REVIEW_ID_INDEX);
				Node review = reviewId_index.get(REVIEW_ID, item_id).getSingle();
				if(review == null)
					throw new KahaniyaCustomException("Invalid review id");
				jsonObject = getJSONForReview(review);
			}
			
			tx.success();
		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_item_details()");
			System.out.println("Something went wrong, while returning item_details  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonObject = new JSONObject();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_item_details()");
			System.out.println("Something went wrong, while returning item_details  :"+ex.getMessage());
			ex.printStackTrace();
			jsonObject = new JSONObject();
		}
		return jsonObject.toString();
	}

	@Override
	public String get_subscriptions_for_user(String user_id, int prev_cnt,
			int count) throws TException {
		JSONArray jsonArray = new JSONArray();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			
			if(user_id == null)
				user_id = "";
			
			int c = 0;

			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Node user = userId_index.get(USER_ID, user_id).getSingle();
			
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			
			ResourceIterator<Node> seriesItr = seriesId_index.query(SERIES_ID, "*").iterator();
			
			LinkedList<Node> seriesList = new LinkedList<Node>();
			while(seriesItr.hasNext())
				seriesList.addLast(seriesItr.next());
			
			Collections.sort(seriesList, TrendingComparatorForSeriesNodes);
		
			LinkedList<Node> outputNodes = new LinkedList<Node>();
			if(user != null)
			{
				Iterator<Relationship> followingSeriesItr = user.getRelationships(USER_SUBSCRIBED_TO_SERIES).iterator();
				LinkedList<Node> followingSeriesList = new LinkedList<Node>();
				while(followingSeriesItr.hasNext())
					followingSeriesList.addLast(followingSeriesItr.next().getEndNode());
				Collections.sort(followingSeriesList, TrendingComparatorForSeriesNodes);
				for(Node series: followingSeriesList)
				{
					if(c < prev_cnt)
					{
						c ++;
						continue;
					}
					if(c >= count + prev_cnt)
						break;
					outputNodes.addLast(series);
					c++;
				}
			}
			
			for(Node series : seriesList)
			{

				if(outputNodes.contains(series) || series.getProperty(SERIES_TYPE).toString().equals("1"))
					continue;
				if(c < prev_cnt)
				{
					c ++;
					continue;
				}
				if(c >= count + prev_cnt)
					break;
				
				outputNodes.addLast(series);
				c++;
			}
			
			for(Node series: outputNodes)
				jsonArray.put(getShortJSONForSeries(series, user));
			
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_subscriptions_for_user()");
			System.out.println("Something went wrong, while returning subscription suggestions from get_subscriptions_for_user  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_subscriptions_for_user()");
			System.out.println("Something went wrong, while returning subscription suggestions get_subscriptions_for_user  :"+ex.getMessage());
			ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		return jsonArray.toString();		

	}
	
	@Override
	public String get_top_authors( int prev_cnt, int count,
			String user_id) throws TException {
		JSONArray jsonArray = new JSONArray();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			
			if(user_id == null)
				user_id = "";
			
			int c = 0;

			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Node user = userId_index.get(USER_ID, user_id).getSingle();

			ResourceIterator<Node> allUsersItr = userId_index.get(USER_ID, "*").iterator();
			
			
			
			LinkedList<Node> authorsList = new LinkedList<Node>();
			while(allUsersItr.hasNext())
			{
				Node n = allUsersItr.next();
				if(n.getDegree(USER_WRITTEN_A_CHAPTER) > 0 && !n.equals(user))
					authorsList.addLast(n);
			}
			
			Collections.sort(authorsList, TrendingComparatorForAuthorNodes);
		
			LinkedList<Node> outputNodes = new LinkedList<Node>();
			for(Node author : authorsList)
			{
				if(c < prev_cnt)
				{
					c ++;
					continue;
				}
				if(c >= count + prev_cnt)
					break;
				outputNodes.addLast(author);
				c++;
			}
			
			for(Node author: outputNodes)
				jsonArray.put(getJSONForUser(author, user));
			
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_top_authors()");
			System.out.println("Something went wrong, while returning top authors from get_top_authors  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_top_authors()");
			System.out.println("Something went wrong, while returning top authors from get_top_authors  :"+ex.getMessage());
			ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		return jsonArray.toString();		

	}

	@Override
	public String get_stats( String stats_type) throws TException {
		JSONObject jsonObj = new JSONObject();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			
			int tot_users = 0;
			int tot_authors = 0;
			
			int tot_series = 0;
			int tot_short_series = 0;
			int tot_series_without_any_chapters = 0;
			int tot_chapters = 0;
			int tot_reviews = 0;
			
			int tot_comments = 0;
			
			JSONObject langInfo = new JSONObject();
			JSONObject genreInfo = new JSONObject();
			
			if("U".equalsIgnoreCase(stats_type))
			{

				Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
				ResourceIterator<Node> allUsersItr = userId_index.query(USER_ID, "*").iterator();
			
				while(allUsersItr.hasNext())
				{
					tot_users++;
					if(allUsersItr.next().hasRelationship(USER_WRITTEN_A_CHAPTER))
						tot_authors++;
				}
				jsonObj.put("Total_Num_Of_Users", tot_users);
				jsonObj.put("Total_Num_Of_Users_Started_Writing", tot_authors);

			}
			else if("S".equalsIgnoreCase(stats_type))
			{
				Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
				ResourceIterator<Node> allSeriesItr = seriesId_index.query(SERIES_ID, "*").iterator();
				while(allSeriesItr.hasNext())
				{
					tot_series++;
					Node series = allSeriesItr.next();
					if(series.getProperty(SERIES_TYPE).toString().equals("1"))
						tot_short_series++;
					if(series.getDegree(CHAPTER_BELONGS_TO_SERIES) == 0)
						tot_series_without_any_chapters++;
				}
				jsonObj.put("Total_Num_Of_Series", tot_series);
				jsonObj.put("Total_Num_Of_Short_Series", tot_short_series);
				jsonObj.put("Total_Num_Of_Series_Without_Stories", tot_series_without_any_chapters);

			}
			else if("CH".equalsIgnoreCase(stats_type))
			{
				Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
				ResourceIterator<Node> allChaptersItr = chapterId_index.query(CHAPTER_ID, "*").iterator();
				while(allChaptersItr.hasNext())
				{
					tot_chapters ++;
					allChaptersItr.next();
				}
				jsonObj.put("Total_Num_Of_Chapters", tot_chapters);
			}
			else if("R".equalsIgnoreCase(stats_type))
			{
				Index<Node> reviewId_index = graphDb.index().forNodes(REVIEW_ID_INDEX);
				ResourceIterator<Node> allReviewsItr = reviewId_index.query(REVIEW_ID, "*").iterator();
				while(allReviewsItr.hasNext())
				{
					tot_reviews ++;
					allReviewsItr.next();
				}
				jsonObj.put("Total_Num_Of_Reviews", tot_reviews);
			}
			else if("C".equalsIgnoreCase(stats_type))
			{
				Index<Node> commentId_index = graphDb.index().forNodes(COMMENT_ID_INDEX);
				ResourceIterator<Node> allCommentsItr = commentId_index.query(COMMENT_ID, "*").iterator();
				while(allCommentsItr.hasNext())
				{
					tot_comments++;
					allCommentsItr.next();
				}
				jsonObj.put("Total_Num_Of_Comments", tot_comments);

			}
			else if("L".equalsIgnoreCase(stats_type))
			{
			
				Index<Node> lang_index = graphDb.index().forNodes(LANG_NAME_INDEX);
				ResourceIterator<Node> allLangsItr = lang_index.query(LANG_NAME, "*").iterator();
				while(allLangsItr.hasNext())
				{
					Node l = allLangsItr.next();
					langInfo.put(l.getProperty(LANG_NAME).toString(), l.getDegree(USER_INTERESTED_LANGUAGE));
				}
				jsonObj.put("lang", langInfo);
			}
			else if("G".equalsIgnoreCase(stats_type))
			{
			
				Index<Node> genre_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
				ResourceIterator<Node> allGenresItr = genre_index.query(GENRE_NAME, "*").iterator();
				while(allGenresItr.hasNext())
				{
					Node g = allGenresItr.next();
					genreInfo.put(g.getProperty(GENRE_NAME).toString(), g.getDegree(USER_INTERESTED_GENRE));
				}
				jsonObj.put("genre", genreInfo);
			}

			
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_stats()");
			System.out.println("Something went wrong, while returning stats from get_stats  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonObj = new JSONObject();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ get_stats()");
			System.out.println("Something went wrong, while returning stats from get_stats  :"+ex.getMessage());
			ex.printStackTrace();
			jsonObj = new JSONObject();
		}
		return jsonObj.toString();		

	}

	@Override
	public String delete_chapter(String chapter_id) throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			
			if(chapter_id == null || chapter_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the parameter chapter_id");

			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> chapterTitleId_index = graphDb.index().forNodes(CHAPTER_TITLE_ID_INDEX);
			Index<Node> search_index = graphDb.index().forNodes(SEARCH_INDEX);
			
			Node chapter = chapterId_index.get(CHAPTER_ID,chapter_id).getSingle();
			
			if(chapter == null)
				throw new KahaniyaCustomException("Chapter doesnot exists with given id : "+chapter_id);

			deleteChapterRelationships(chapter);
			
			chapterId_index.remove(chapter);
			chapterTitleId_index.remove(chapter);
			search_index.remove(chapter);
			chapter.delete();
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_chapter()");
			System.out.println("Something went wrong, while deleting chapter from delete_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_chapter()");
			System.out.println("Something went wrong, while deleting chapter from delete_chapter  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;	
	}
	
	private void deleteChapterRelationships(Node chapter)
	{

		Index<Relationship> chapterViewsIndex = graphDb.index().forRelationships(USER_VIEWED_CHAPTER_REL_INDEX);
		Index<Node> commentId_index = graphDb.index().forNodes(COMMENT_ID_INDEX);
		
		Iterator<Relationship> relsItr = chapter.getRelationships(USER_VIEWED_A_CHAPTER).iterator();
		while(relsItr.hasNext())
			chapterViewsIndex.remove(relsItr.next());

		relsItr = chapter.getRelationships(COMMENT_WRITTEN_ON_CHAPTER).iterator();
		while(relsItr.hasNext())
		{
			Relationship rel =  relsItr.next();
			Node cmntNode = rel.getStartNode();
			rel.delete();
			
			commentId_index.remove(cmntNode);
			deleteSubComments(cmntNode);
			Iterator<Relationship> cmntRels = cmntNode.getRelationships().iterator();
			while(cmntRels.hasNext())
				cmntRels.next().delete();
			cmntNode.delete();
		}

		
		relsItr = chapter.getRelationships().iterator();
		while(relsItr.hasNext())
			relsItr.next().delete();
	}
	
	private void deleteSeriesRelationships(Node series)
	{

		Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
		Index<Node> reviewId_index = graphDb.index().forNodes(REVIEW_ID_INDEX);

		Iterator<Relationship> relsItr = series.getRelationships(REVIEW_BELONGS_TO_SERIES).iterator();
		while(relsItr.hasNext())
		{
			Relationship rel =  relsItr.next();
			Node reviewNode = rel.getStartNode();
			rel.delete();
			
			reviewId_index.remove(reviewNode);
			Iterator<Relationship> reviewRels = reviewNode.getRelationships().iterator();
			while(reviewRels.hasNext())
				reviewRels.next().delete();
			reviewNode.delete();
		}
		
		relsItr = series.getRelationships(CHAPTER_BELONGS_TO_SERIES).iterator();
		while(relsItr.hasNext())
		{
			Relationship rel =  relsItr.next();
			Node chapterNode = rel.getStartNode();
			rel.delete();
			
			chapterId_index.remove(chapterNode);
			deleteChapterRelationships(chapterNode);
			chapterNode.delete();
		}

		
		relsItr = series.getRelationships().iterator();
		while(relsItr.hasNext())
			relsItr.next().delete();
	}
	
	private void deleteSubComments(Node comment)
	{

		Index<Node> commentId_index = graphDb.index().forNodes(COMMENT_ID_INDEX);
		
		Iterator<Relationship> relItr = comment.getRelationships(REPLY_COMMENT_WRITTEN_ON_COMMENT).iterator();
		while(relItr.hasNext())
		{
			Relationship rel = relItr.next();
			Node cmntNode = rel.getStartNode();
			rel.delete();
			
			deleteSubComments(cmntNode);
			commentId_index.remove(cmntNode);

			Iterator<Relationship> cmntRels = cmntNode.getRelationships().iterator();
			while(cmntRels.hasNext())
				cmntRels.next().delete();
			
			cmntNode.delete();
		}
		
	}

	@Override
	public String delete_user(String user_id) throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			
			if(user_id == null || user_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the parameter user_id");

			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> search_index = graphDb.index().forNodes(SEARCH_INDEX);
			
			Node user = userId_index.get(USER_ID,user_id).getSingle();
			
			if(user == null)
				throw new KahaniyaCustomException("User doesnot exists with given id : "+user_id);
			
			userId_index.remove(user);
			search_index.remove(user);
			user.setProperty(IS_DELETED,"1");
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_series()");
			System.out.println("Something went wrong, while deleting series from delete_series  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_series()");
			System.out.println("Something went wrong, while deleting series from delete_series  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;	
	}

	@Override
	public String delete_series(String series_id) throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			
			if(series_id == null || series_id.length() == 0)
				throw new KahaniyaCustomException("Null or empty string receieved for the parameter series_id");

			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> seriesTitleId_index = graphDb.index().forNodes(SERIES_TITLE_ID_INDEX);
			Index<Node> seriesType_index = graphDb.index().forNodes(SERIES_TYPE_INDEX);
			Index<Node> search_index = graphDb.index().forNodes(SEARCH_INDEX);
			
			Node series = seriesId_index.get(SERIES_ID,series_id).getSingle();
			
			if(series == null)
				throw new KahaniyaCustomException("Series doesnot exists with given id : "+series_id);
			
			deleteSeriesRelationships(series);

			seriesId_index.remove(series);
			seriesTitleId_index.remove(series);
			seriesType_index.remove(series);
			search_index.remove(series);
			series.delete();
			
			res = "true";
			tx.success();

		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_series()");
			System.out.println("Something went wrong, while deleting series from delete_series  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ delete_series()");
			System.out.println("Something went wrong, while deleting series from delete_series  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;	
	}

	
	@Override
	public String search(String query, int tp, String user_id, int prev_cnt, int count)
			throws TException {
		JSONArray jsonArray = new JSONArray();		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);

			if(query == null || query.length() == 0)
				throw new KahaniyaCustomException("Empty value receieved for the parameter query");

			Index<Node> user_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> search_index = graphDb.index().forNodes(SEARCH_INDEX);
			ResourceIterator<Node> userItr = null;
			ResourceIterator<Node> seriesItr = null;
			ResourceIterator<Node> chapterItr = null;
			
			if(tp == 0) //all
			{
				userItr = search_index.query(SEARCH_USER, "*"+query.toLowerCase()+"*").iterator();
				seriesItr = search_index.query(SEARCH_SERIES, "*"+query.toLowerCase()+"*").iterator();
				chapterItr = search_index.query(SEARCH_CHAPTER, "*"+query.toLowerCase()+"*").iterator();
				int  i = 0;
				JSONArray usersArray = new JSONArray();
				while(userItr.hasNext() && i < 3)
				{
					JSONObject obj = new JSONObject();
					Node user = userItr.next();
					obj.put("FullName", user.getProperty(FULL_NAME).toString());
					obj.put("UserId", user.getProperty(USER_ID).toString());
					usersArray.put(obj);
					i++;
				}
				JSONObject usersData = new JSONObject();
				usersData.put("tp", 1);
				usersData.put("data", usersArray);
				
				i = 0;
				JSONArray seriesArray = new JSONArray();
				while(seriesItr.hasNext() && i < 3)
				{
					JSONObject obj1 = new JSONObject();
					Node series = seriesItr.next();
					obj1.put("P_Title", series.getProperty(SERIES_TITLE).toString());
					obj1.put("P_Id", series.getProperty(SERIES_ID).toString());
					obj1.put("P_Title_ID", series.getProperty(SERIES_TITLE_ID).toString());
					seriesArray.put(obj1);
					i++;
				}
				JSONObject seriesData = new JSONObject();
				seriesData.put("tp", 2);
				seriesData.put("data", seriesArray);

				i = 0;
				JSONArray chapterArray = new JSONArray();
				while(chapterItr.hasNext() && i < 3)
				{
					JSONObject obj2 = new JSONObject();
					Node chapter = chapterItr.next();
					obj2.put("P_Title", chapter.getProperty(CHAPTER_TITLE).toString());
					obj2.put("P_Id", chapter.getProperty(CHAPTER_ID).toString());
					obj2.put("P_Title_ID", chapter.getProperty(CHAPTER_TITLE_ID).toString());
					obj2.put("S_Title_ID", chapter.getSingleRelationship(CHAPTER_BELONGS_TO_SERIES, Direction.OUTGOING).getEndNode().getProperty(SERIES_TITLE_ID).toString());
					chapterArray.put(obj2);
					i++;
				}
				JSONObject chaptersData = new JSONObject();
				chaptersData.put("tp", 3);
				chaptersData.put("data", chapterArray);
				
				jsonArray.put(usersData);
				jsonArray.put(seriesData);
				jsonArray.put(chaptersData);
				
			}
			else if(tp == 1) // users
			{
				userItr = search_index.query(SEARCH_USER, "*"+query.toLowerCase()+"*").iterator();
				Node s_user = user_index.get(USER_ID, user_id).getSingle();
				
				int c = 0;
				while(c < prev_cnt && userItr.hasNext())
				{
					c++;
					userItr.next();
				}
				c = 0;
				while(userItr.hasNext() && c < count)
				{
					c++;
					jsonArray.put(getJSONForUser(userItr.next(),s_user)); 
				}
			}
			else if(tp == 2) // series
			{
				seriesItr = search_index.query(SEARCH_SERIES, "*"+query.toLowerCase()+"*").iterator();
				Node s_user = user_index.get(USER_ID, user_id).getSingle();
				
				int c = 0;
				while(c < prev_cnt && seriesItr.hasNext())
				{
					c++;
					seriesItr.next();
				}
				c = 0;
				while(seriesItr.hasNext() && c < count)
				{
					c++;
					jsonArray.put(getJSONForSeries(seriesItr.next(),s_user)); 
				}
			}
			else if(tp == 3) // chapters
			{
				chapterItr = search_index.query(SEARCH_CHAPTER, "*"+query.toLowerCase()+"*").iterator();
				Node s_user = user_index.get(USER_ID, user_id).getSingle();

				int c = 0;
				while(c < prev_cnt && chapterItr.hasNext())
				{
					c++;
					chapterItr.next();
				}
				c = 0;
				while(chapterItr.hasNext() && c < count)
				{
					c++;
					jsonArray.put(getJSONForChapter(chapterItr.next(),s_user)); 
				}
			}
			
			tx.success();
		}
		catch(KahaniyaCustomException ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ search()");
			System.out.println("Something went wrong, while returning search  :"+ex.getMessage());
//				ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		catch(Exception ex)
		{
			System.out.println(new Date().toString());
			System.out.println("Exception @ search()");
			System.out.println("Something went wrong, while returning search  :"+ex.getMessage());
			ex.printStackTrace();
			jsonArray = new JSONArray();
		}
		return jsonArray.toString();	
	}

}
