package kahania;

import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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

import kahania.KahaniaCustomException;

public class Kahania implements KahaniaService.Iface{

	
	//Index names
	public static final String GENRE_NAME_INDEX = "genre_index_by_name";
	public static final String LANG_NAME_INDEX = "language_index_by_name";
	public static final String USER_ID_INDEX = "user_index_by_id";
	public static final String USER_NAME_INDEX = "user_index_by_uname";
	public static final String USER_EMAIL_INDEX = "user_index_by_email";

	public static final String SERIES_ID_INDEX = "series_index_by_id";
	public static final String REVIEW_ID_INDEX = "review_index_by_id";
	public static final String CHAPTER_ID_INDEX = "chapter_index_by_id";
	public static final String CHAPTER_TITLE_ID_INDEX = "chapter_index_by_title_id";
	public static final String SERIES_TITLE_ID_INDEX = "series_index_by_title_id";
	public static final String SERIES_TYPE_INDEX = "series_index_by_type";
	
	public static final String USER_VIEWED_CHAPTER_REL_INDEX = "user_viewed_chapter_relation_index";
	
	public static final String KEYWORD_INDEX = "keyword_index_by_name";
	
	public static final String LOCK_INDEX = "lock_index";
	
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
	public static final String DOB = "dob";
	public static final String GENDER = "gender";
	public static final String PRIVILEGE = "privilege";
	public static final String STATUS = "status";
	public static final String TIME_CREATED = "time_created";
	public static final String TIME_EDITED = "time_edited";
	
	//genre node related keys
	public static final String GENRE_NAME = "genre_name";

	//language node related keys
	public static final String LANG_NAME = "language_name";
	
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
	
	public static final String NODE_TYPE = "node_type";
	public static final String USER_NODE = "user_node";
	public static final String GENRE_NODE = "genre_node";
	public static final String LANG_NODE = "language_node";
	public static final String SERIES_NODE = "series_node";
	public static final String KEYWORD_NODE = "keyword_node";
	public static final String REVIEW_NODE = "review_node";
	public static final String CHAPTER_NODE = "chapter_node";

	private static GraphDatabaseService graphDb = null;
	
	//Relationship names
	RelationshipType USER_INTERESTED_GENRE = DynamicRelationshipType.withName("USER_INTERESTED_GENRE");
	RelationshipType USER_INTERESTED_LANGUAGE = DynamicRelationshipType.withName("USER_INTERESTED_LANGUAGE");
	RelationshipType USER_FOLLOW_USER = DynamicRelationshipType.withName("USER_FOLLOW_USER");
	RelationshipType USER_STARTED_SERIES = DynamicRelationshipType.withName("USER_STARTED_SERIES");

	RelationshipType SERIES_BELONGS_TO_GENRE = DynamicRelationshipType.withName("SERIES_BELONGS_TO_GENRE");
	RelationshipType SERIES_BELONGS_TO_LANGUAGE = DynamicRelationshipType.withName("SERIES_BELONGS_TO_LANGUAGE");
	RelationshipType SERIES_KEYWORD = DynamicRelationshipType.withName("SERIES_KEYWORD");
	
	RelationshipType USER_WRITTEN_A_REVIEW = DynamicRelationshipType.withName("USER_WRITTEN_A_REVIEW");
	RelationshipType REVIEW_BELONGS_TO_SERIES = DynamicRelationshipType.withName("REVIEW_BELONGS_TO_SERIES");
	RelationshipType USER_SUBSCRIBED_TO_SERIES = DynamicRelationshipType.withName("USER_SUBSCRIBED_TO_SERIES");	
	RelationshipType USER_FAV_CHAPTER = DynamicRelationshipType.withName("USER_FAV_CHAPTER");
	RelationshipType USER_RATED_A_CHAPTER = DynamicRelationshipType.withName("USER_RATED_A_CHAPTER");
	RelationshipType USER_VIEWED_A_CHAPTER = DynamicRelationshipType.withName("USER_VIEWED_A_CHAPTER");	
	RelationshipType USER_WRITTEN_A_CHAPTER = DynamicRelationshipType.withName("USER_WRITTEN_A_CHAPTER");	
	RelationshipType CHAPTER_BELONGS_TO_SERIES = DynamicRelationshipType.withName("CHAPTER_BELONGS_TO_SERIES");	
	
	private static Comparator<Node> TimeCreatedComparatorForNodes = new Comparator<Node>() {
		public int compare(Node n1, Node n2) {
		   int v1 = 0;
		   int v2 = 0;
		   if(n1.hasProperty(TIME_CREATED))
			   v1 = (int)n1.getProperty(TIME_CREATED);
		   if(n2.hasProperty(TIME_CREATED))
			   v2 = (int)n2.getProperty(TIME_CREATED);
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
			   v1 = (int)n1.getProperty(TIME_CREATED);
		   if(n2.hasProperty(TIME_CREATED))
			   v2 = (int)n2.getProperty(TIME_CREATED);
		   //ascending order
		   //return v1-v2;
		   //descending order
		   return v2-v1;
	    }
	}; 

    private Node User(String id, String full_name, String user_name, String email, String mobile, String dob, int privilege, int status, int time_created){
		Node node = graphDb.createNode();
		node.setProperty(USER_ID,id);
	    node.setProperty(FULL_NAME,full_name);
		node.setProperty(USER_NAME,user_name);
		node.setProperty(EMAIL,email);
		node.setProperty(MOBILE_NUMBER,mobile);
		node.setProperty(DOB,dob);
		node.setProperty(GENDER,"NA");
		node.setProperty(PRIVILEGE,privilege);
		node.setProperty(STATUS,status);
	    node.setProperty(TIME_CREATED,time_created);
	    node.setProperty(NODE_TYPE, USER_NODE);
	    return node;
	} 

    private Node Genre(String name){
		Node node = graphDb.createNode();
		node.setProperty(GENRE_NAME,name);
	    node.setProperty(NODE_TYPE, GENRE_NODE);
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
			
			TServerSocket serverTransport = new TServerSocket(9779);
			KahaniaService.Processor<KahaniaService.Iface> processor = new KahaniaService.Processor<KahaniaService.Iface>(this);
			Args serverArgs = new Args(serverTransport);
			serverArgs.processor(processor);
			TServer server = new TThreadPoolServer(serverArgs);
			System.out.println("Kahania thrift service is started");
			server.serve();
			
		}catch(Exception e)
		{
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
		      System.out.println("Something went wrong, while jar_shutdown :"+ex.getMessage());
		      ex.printStackTrace();
		      res = "Failed to terminate";
		   }
		return res;
	}
	
	public Kahania()
	{
		if(graphDb == null)
			initGraphDb();
	
	}
	
	private static void initGraphDb()
	{
		//db path
		String storeDir = "/kahania/n4j/data/graph.db";
		
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
								USER_ID_INDEX,
								USER_NAME_INDEX,
								USER_EMAIL_INDEX,
								SERIES_ID_INDEX,
								REVIEW_ID_INDEX,
								CHAPTER_ID_INDEX,
								SERIES_TITLE_ID_INDEX,
								SERIES_TYPE_INDEX,
								KEYWORD_INDEX,
								LOCK_INDEX
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
			System.out.println("Exception @ add_neo4j_lock_nodes()");
			System.out.println("Failed to create lock nodes : " + e.getMessage());}
		finally{}
	}
	
	private Lock aquireWriteLock(Transaction tx) throws Exception {
		Index<Node> lockNodeIndex = graphDb.index().forNodes( LOCK_INDEX );
		Node tobeLockedNode = lockNodeIndex.get( LockName, LockName ).getSingle();
		if(tobeLockedNode == null)
	      throw new RuntimeException("Locking node for "+LockName+" not found, unbale to synchronize the call.");
		return tx.acquireWriteLock(tobeLockedNode);  //lock simultaneous execution of create_comment to avoid duplicate comment creation
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
			
			if(name == null || name.length() == 0)
				throw new KahaniaCustomException("Null or empty string found for genre name");
			
			name = name.toLowerCase();
			
			if(genreName_index.get(GENRE_NAME,name).getSingle()!=null)
				throw new KahaniaCustomException("Genre already exists with given name : "+name);
			
			Node genre_node = Genre(name);  // Creating a new genre node
			if(genre_node == null)
				throw new KahaniaCustomException("Something went wrong, while creating genre with given name");

			//Indexing newly created genre node
			genreName_index.add(genre_node, GENRE_NAME, name);
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ create_genre('"+name+"')");
			System.out.println("Something went wrong, while creating genre from create_genre  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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

			if(old_name == null || old_name.length() == 0)
				throw new KahaniaCustomException("Null or empty string found for genre existing name");
			if(new_name == null || new_name.length() == 0)
				throw new KahaniaCustomException("Null or empty string found for new genre name");
			
			old_name = old_name.toLowerCase();
			new_name = new_name.toLowerCase();
						
			Node genre_node = genreName_index.get(GENRE_NAME,old_name).getSingle();
	
			if(genre_node == null)
				throw new KahaniaCustomException("Genre doesnot exists with given old name");

			if(genreName_index.get(GENRE_NAME,new_name).getSingle() != null)
				throw new KahaniaCustomException("Genre already exists with given new name");

			genre_node.setProperty(GENRE_NAME, old_name);
			//Update indexing for genre node
			genreName_index.remove(genre_node);
			genreName_index.add(genre_node, GENRE_NAME, new_name);
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ edit_genre('"+old_name+"','"+new_name+"')");
			System.out.println("Something went wrong, while editing genre from edit_genre  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			
			if(name == null || name.length() == 0)
				throw new KahaniaCustomException("Null or empty string found for genre name");
			
			name = name.toLowerCase();
			
			Node genre_node = genreName_index.get(GENRE_NAME,name).getSingle();
			
			if(genre_node == null)
				throw new KahaniaCustomException("Genre node doesnot exists with given name");

			//Remove relationships for genre node
//			for(Relationship rel : genre_node.getRelationships())
//				rel.delete();

			if(genre_node.getDegree(SERIES_BELONGS_TO_GENRE) > 0)
				throw new KahaniaCustomException("Genre node cannot be deleted, there exists some series related to given genre : " + name );
			
			//Remove indexing for genre node
			genreName_index.remove(genre_node);
			genre_node.delete();
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ delete_genre('"+name+"')");
			System.out.println("Something went wrong, while deleting genre from delete_genre  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ list_genres()");
			System.out.println("Something went wrong, while returning genres  :"+ex.getMessage());
//			ex.printStackTrace();

		}
		catch(Exception ex)
		{
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
			
			if(name == null || name.length() == 0)
				throw new KahaniaCustomException("Null or empty string found for language name");
			
			name = name.toLowerCase();
			
			if(langName_index.get(LANG_NAME,name).getSingle()!=null)
				throw new KahaniaCustomException("Lang already exists with given name : "+name);

			Node lang_node = Language(name);  // Creating a new lang node
			if(lang_node == null)
				throw new KahaniaCustomException("Something went wrong, while creating language with given name");

			//Indexing newly created lang node
			langName_index.add(lang_node, LANG_NAME, name);
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ create_language('"+name+"')");
			System.out.println("Something went wrong, while creating language from create_language  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			if(old_name == null || old_name.length() == 0)
				throw new KahaniaCustomException("Null or empty string found for old language name");
			if(new_name == null || new_name.length() == 0)
				throw new KahaniaCustomException("Null or empty string found for new language name");
			
			old_name = old_name.toLowerCase();
			new_name = new_name.toLowerCase();
			
			Node lang_node = langName_index.get(LANG_NAME,old_name).getSingle();
			
			if(lang_node == null)
				throw new KahaniaCustomException("Lang doesnot exists with given old name");

			if(langName_index.get(LANG_NAME,new_name).getSingle() != null)
				throw new KahaniaCustomException("Lang already exists with given new name");
			
			lang_node.setProperty(LANG_NAME, old_name);
			//Update indexing for lang node
			langName_index.remove(lang_node);
			langName_index.add(lang_node, LANG_NAME, new_name);
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ edit_language('"+old_name+"','"+new_name+"')");
			System.out.println("Something went wrong, while editing language from edit_language  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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

			if(name == null || name.length() == 0)
				throw new KahaniaCustomException("Null or empty string found for language name");
			
			name = name.toLowerCase();

			Node lang_node = langName_index.get(LANG_NAME,name).getSingle();
			
			if(lang_node == null)
				throw new KahaniaCustomException("Lang node doesnot exists with given name");

			//Remove relationships for lang node
//			for(Relationship rel : lang_node.getRelationships())
//				rel.delete();
			if(lang_node.getDegree(SERIES_BELONGS_TO_LANGUAGE) > 0)
				throw new KahaniaCustomException("Lang node cannot be deleted, there exists some series related to given lang : " + name );

			//Remove indexing for lang node
			langName_index.remove(lang_node);
			lang_node.delete();
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ delete_language('"+name+"')");
			System.out.println("Something went wrong, while deleting language from delete_language  :"+ex.getMessage());
//			ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ list_languages()");
			System.out.println("Something went wrong, while returning languages  :"+ex.getMessage());
//			ex.printStackTrace();

		}
		catch(Exception ex)
		{
			System.out.println("Exception @ list_languages()");
			System.out.println("Something went wrong, while returning languages  :"+ex.getMessage());
			ex.printStackTrace();
		}
		
		return res.toString();
	}
	
	@Override
	public String create_user(String id, String full_name, String user_name,
			String email, String mobile_number, String dob, String genres,
			String languages, int privilege, int status, int time_created)
			throws TException {

		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> userName_index = graphDb.index().forNodes(USER_NAME_INDEX);
			Index<Node> userEmail_index = graphDb.index().forNodes(USER_EMAIL_INDEX);
			
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			
			if(userId_index.get(USER_ID,id).getSingle()!=null)
				throw new KahaniaCustomException("User already exists with given id : "+id);
			if(userName_index.get(USER_NAME,user_name).getSingle()!=null)
				throw new KahaniaCustomException("User already exists with given user_name : "+user_name);
			if(userEmail_index.get(EMAIL,email).getSingle()!=null)
				throw new KahaniaCustomException("User already exists with given email : "+email);

			Node user_node = User(id, full_name, user_name, email, mobile_number, dob, privilege, status, time_created);  // Creating a new user node
			if(user_node == null)
				throw new KahaniaCustomException("Something went wrong, while creating user ");

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
			userName_index.add(user_node, USER_NAME, user_name);
			userEmail_index.add(user_node, EMAIL, email);

			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ create_user()");
			System.out.println("Something went wrong, while creating user from create_user  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			
			Node user_node = userId_index.get(USER_ID,id).getSingle();
			if(user_node == null)
				throw new KahaniaCustomException("User does not exists with given id : "+id);
			
			user_node.setProperty(FULL_NAME, full_name);
			user_node.setProperty(GENDER, gender);
			user_node.setProperty(DOB, dob);
			
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ edit_user_basic_info()");
			System.out.println("Something went wrong, while editing user from edit_user_basic_info  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> userEmail_index = graphDb.index().forNodes(USER_EMAIL_INDEX);
			
			Node user_node = userId_index.get(USER_ID,id).getSingle();
			if(user_node == null)
				throw new KahaniaCustomException("User does not exists with given id : "+id);
			
			user_node.setProperty(EMAIL, email);
			user_node.setProperty(MOBILE_NUMBER, mobile_number);
			
			userEmail_index.remove(user_node);
			userEmail_index.add(user_node, EMAIL, email);
			
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ edit_user_contact_details()");
			System.out.println("Something went wrong, while editing user from edit_user_contact_details  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> userName_index = graphDb.index().forNodes(USER_NAME_INDEX);
			
			Node user_node = userId_index.get(USER_ID,id).getSingle();
			if(user_node == null)
				throw new KahaniaCustomException("User does not exists with given id : "+id);
			
			user_node.setProperty(USER_NAME, user_name);
			
			userName_index.remove(user_node);
			userName_index.add(user_node, USER_NAME, user_name);
			
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ edit_user_security_details()");
			System.out.println("Something went wrong, while editing user from edit_user_security_details  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			
			Node user_node = userId_index.get(USER_ID,id).getSingle();
			if(user_node == null)
				throw new KahaniaCustomException("User does not exists with given id : "+id);
			
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
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ edit_user_languages()");
			System.out.println("Something went wrong, while editing user from edit_user_languages  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			
			Node user_node = userId_index.get(USER_ID,id).getSingle();
			if(user_node == null)
				throw new KahaniaCustomException("User does not exists with given id : "+id);
			
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
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ edit_user_genres()");
			System.out.println("Something went wrong, while editing user from edit_user_genres  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			
			Index<Node> userIdIndex = graphDb.index().forNodes(USER_ID_INDEX);
			Node userOne = userIdIndex.get(USER_ID, user_id_1).getSingle();
			Node userTwo = userIdIndex.get(USER_ID, user_id_2).getSingle();

			if(userOne == null)
				throw new KahaniaCustomException("User does not exists with given id : " + user_id_1);
			if(userTwo == null)
				throw new KahaniaCustomException("User does not exists with given id : " + user_id_2);
			
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
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ follow_user()");
			System.out.println("Something went wrong, while following user from follow_user  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
		ret.put("type", "lang");
		ret.put("Obj",obj);
		return ret;
	}
	
	private JSONObject getJSONForLang(Node genre)
	{
		JSONObject ret = new JSONObject();
		
			JSONObject obj = new JSONObject();
			obj.put("name", genre.getProperty(LANG_NAME).toString());
		ret.put("type", "lang");
		ret.put("Obj",obj);
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
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> seriesTitleId_index = graphDb.index().forNodes(SERIES_TITLE_ID_INDEX);
			Index<Node> seriesType_index = graphDb.index().forNodes(SERIES_TYPE_INDEX);
			
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			
			Index<Node> keyword_index = graphDb.index().forNodes(KEYWORD_INDEX);
			
			Node userNode = userId_index.get(USER_ID,user_id).getSingle();
			if(userNode == null)
				throw new KahaniaCustomException("User doesnot exists with given id : "+user_id);
			if(seriesId_index.get(SERIES_ID,series_id).getSingle()!=null)
				throw new KahaniaCustomException("Series already exists with given id : "+series_id);
			if(seriesTitleId_index.get(SERIES_TITLE_ID,title_id).getSingle()!=null)
				throw new KahaniaCustomException("Series already exists with given title id : "+title_id);

			//validate genre and language
			if(language == null || language.length() == 0)
				throw new KahaniaCustomException("Null or Empty string receieved for the param language");
			if(genre == null || genre.length() == 0)
				throw new KahaniaCustomException("Null or Empty string receieved for the param genre");
			
			Node genreNode = genreName_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
			if(genreNode == null)
				throw new KahaniaCustomException("Genre doesnot exists for the name : " + genre);
			
			Node langNode = langName_index.get(LANG_NAME, language.toLowerCase()).getSingle();
			if(langNode == null)
				throw new KahaniaCustomException("Language doesnot exists for the name : " + language);
			
			Node series_node = Series(series_id, title, title_id, tag_line, feature_image, keywords, copyrights, dd_img, dd_summary, series_type, time_created);  // Creating a new series node
			if(series_node == null)
				throw new KahaniaCustomException("Something went wrong, while creating series ");

			//create relationship with user
			createRelation(USER_STARTED_SERIES, userNode, series_node);
			
			//create relationship with keywords
			keywords = keywords.toLowerCase();
			for(String keyword : keywords.split(","))
			{
				Node keyword_node = keyword_index.get(KEYWORD_NAME, keyword).getSingle();
				if(keyword_node == null)
				{
					keyword_node = Keyword(keyword);
					keyword_index.add(keyword_node, KEYWORD_NAME, keyword);
				}
				createRelation(SERIES_KEYWORD, series_node, keyword_node);
				
			}
			//create relationships with Genres and Languages
			createRelation(SERIES_BELONGS_TO_GENRE, series_node, genreNode);
			createRelation(SERIES_BELONGS_TO_LANGUAGE, series_node, langNode);
			
			//Indexing newly created series node
			seriesId_index.add(series_node, SERIES_ID, series_id);
			seriesTitleId_index.add(series_node, SERIES_TITLE_ID, title_id);
			seriesType_index.add(series_node, SERIES_TYPE, series_type);

			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ create_series()");
			System.out.println("Something went wrong, while creating series from create_series  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> seriesType_index = graphDb.index().forNodes(SERIES_TYPE_INDEX);
			
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);

			Index<Node> keyword_index = graphDb.index().forNodes(KEYWORD_INDEX);
			
			Node series_node = seriesId_index.get(SERIES_ID, series_id).getSingle();
			if(series_node == null)
				throw new KahaniaCustomException("Series doesnot exists with given id : "+series_id);
			
			//validate genre and language
			if(language == null || language.length() == 0)
				throw new KahaniaCustomException("Null or Empty string receieved for the param language");
			if(genre == null || genre.length() == 0)
				throw new KahaniaCustomException("Null or Empty string receieved for the param genre");

			Node genreNode = genreName_index.get(GENRE_NAME, genre.toLowerCase()).getSingle();
			if(genreNode == null)
				throw new KahaniaCustomException("Genre doesnot exists for the name : " + genre);
			
			Node langNode = langName_index.get(LANG_NAME, language.toLowerCase()).getSingle();
			if(langNode == null)
				throw new KahaniaCustomException("Language doesnot exists for the name : " + language);
			
			//remove existing relationships with Genres, Languages and keywords
			for(Relationship rel : series_node.getRelationships(SERIES_BELONGS_TO_GENRE))
				rel.delete();
			for(Relationship rel : series_node.getRelationships(SERIES_BELONGS_TO_LANGUAGE))
				rel.delete();
			for(Relationship rel : series_node.getRelationships(SERIES_KEYWORD))
				rel.delete();

			//create relationships with Genres and Languages
			createRelation(SERIES_BELONGS_TO_GENRE, series_node, genreNode, Integer.parseInt(series_node.getProperty(TIME_CREATED).toString()));
			createRelation(SERIES_BELONGS_TO_LANGUAGE, series_node, langNode, Integer.parseInt(series_node.getProperty(TIME_CREATED).toString()));
			
			//create relationship with keywords
			keywords = keywords.toLowerCase();
			for(String keyword : keywords.split(","))
			{
				Node keyword_node = keyword_index.get(KEYWORD_NAME, keyword).getSingle();
				if(keyword_node == null)
				{
					keyword_node = Keyword(keyword);
					keyword_index.add(keyword_node, KEYWORD_NAME, keyword);
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
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ edit_series()");
			System.out.println("Something went wrong, while editing series from edit_series  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> reviewId_index = graphDb.index().forNodes(REVIEW_ID_INDEX);
			
			Node userNode = userId_index.get(USER_ID,user_id).getSingle();
			if(userNode == null)
				throw new KahaniaCustomException("User doesnot exists with given id : "+user_id);
			Node seriesNode = seriesId_index.get(SERIES_ID,series_id).getSingle();
			if(seriesNode == null)
				throw new KahaniaCustomException("Series doesnot exists with given id : "+series_id);
			
			if(reviewId_index.get(REVIEW_ID,review_id).getSingle()!=null)
				throw new KahaniaCustomException("Review already exists with given id : "+review_id);
			
			Node review_node = Review(review_id, data, time_created);  // Creating a new review node
			if(review_node == null)
				throw new KahaniaCustomException("Something went wrong, while creating review ");

			//create relationship with user
			createRelation(USER_WRITTEN_A_REVIEW, userNode, review_node, time_created);
			
			//create relationships with Series
			createRelation(REVIEW_BELONGS_TO_SERIES, review_node, seriesNode, time_created);
			
			//Indexing newly created series node
			reviewId_index.add(review_node, REVIEW_ID, review_id);
			
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ create_review()");
			System.out.println("Something went wrong, while creating review from create_review  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			
			Index<Node> reviewId_index = graphDb.index().forNodes(REVIEW_ID_INDEX);
			Node review_node = reviewId_index.get(REVIEW_ID, review_id).getSingle();
			if(review_node == null)
				throw new KahaniaCustomException("Review doesnot exists with given id : "+review_id);
			
			review_node.setProperty(REVIEW_DATA, data);

			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ edit_review()");
			System.out.println("Something went wrong, while editing review from edit_review  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			
			Node userNode = userId_index.get(USER_ID,user_id).getSingle();
			if(userNode == null)
				throw new KahaniaCustomException("User doesnot exists with given id : "+user_id);
			Node seriesNode = seriesId_index.get(SERIES_ID,series_id).getSingle();
			if(seriesNode == null)
				throw new KahaniaCustomException("Series doesnot exists with given id : "+series_id);

			Node chapterNode = chapterId_index.get(CHAPTER_ID,chapter_id).getSingle();
			if(chapterNode == null)
				throw new KahaniaCustomException("Chapter doesnot exists with given id : "+chapter_id);

			chapterNode.setProperty(CHAPTER_TITLE, title);
			chapterNode.setProperty(CHAPTER_FEAT_IMAGE, feat_image);
			chapterNode.setProperty(CHAPTER_FREE_OR_PAID, free_or_paid);
			
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ edit_chapter()");
			System.out.println("Something went wrong, while editing chapter from edit_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> chapterTitleId_index = graphDb.index().forNodes(CHAPTER_TITLE_ID_INDEX);
			
			Node userNode = userId_index.get(USER_ID,user_id).getSingle();
			if(userNode == null)
				throw new KahaniaCustomException("User doesnot exists with given id : "+user_id);
			Node seriesNode = seriesId_index.get(SERIES_ID,series_id).getSingle();
			if(seriesNode == null)
				throw new KahaniaCustomException("Series doesnot exists with given id : "+series_id);

			if(chapterId_index.get(CHAPTER_ID,chapter_id).getSingle()!=null)
				throw new KahaniaCustomException("Chapter already exists with given id : "+chapter_id);
			
			if(chapterTitleId_index.get(CHAPTER_TITLE_ID,title_id).getSingle()!=null)
				throw new KahaniaCustomException("Chapter already exists with given title id : "+title_id);
			
			Node chapter_node = Chapter(chapter_id, title_id, title, feat_image, free_or_paid, time_created);  // Creating a new chapter node
			if(chapter_node == null)
				throw new KahaniaCustomException("Something went wrong, while creating chapter ");

			//create relationship with user
			createRelation(USER_WRITTEN_A_CHAPTER, userNode, chapter_node, time_created);
			
			//create relationships with Series
			createRelation(CHAPTER_BELONGS_TO_SERIES, chapter_node, seriesNode, time_created);
			
			//Indexing newly created series node
			chapterId_index.add(chapter_node, CHAPTER_ID, chapter_id);
			chapterTitleId_index.add(chapter_node, CHAPTER_TITLE_ID, title_id);
			
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ create_chapter()");
			System.out.println("Something went wrong, while creating chapter from create_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> seriesId_index = graphDb.index().forNodes(SERIES_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			
			Node series_node = seriesId_index.get(SERIES_ID, series_id).getSingle();
			if(series_node == null)
				throw new KahaniaCustomException("Series doesnot exists with given id : "+series_id);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			if(user_node == null)
				throw new KahaniaCustomException("User doesnot exists with given id : "+user_id);
			
			if(isRelationExistsBetween(USER_SUBSCRIBED_TO_SERIES, user_node, series_node))
				deleteRelation(USER_SUBSCRIBED_TO_SERIES, user_node, series_node);
			else
				createRelation(USER_SUBSCRIBED_TO_SERIES, user_node, series_node, time);
						
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ subscribe_series()");
			System.out.println("Something went wrong, while subscribing series from subscribe_series  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			
			Node chapter_node = chapterId_index.get(CHAPTER_ID, chapter_id).getSingle();
			if(chapter_node == null)
				throw new KahaniaCustomException("Chapter doesnot exists with given id : "+chapter_id);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			if(user_node == null)
				throw new KahaniaCustomException("User doesnot exists with given id : "+user_id);
			
			if(isRelationExistsBetween(USER_FAV_CHAPTER, user_node, chapter_node))
				deleteRelation(USER_FAV_CHAPTER, user_node, chapter_node);
			else
				createRelation(USER_FAV_CHAPTER, user_node, chapter_node, time);
						
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
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
			aquireWriteLock(tx);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			
			Node chapter_node = chapterId_index.get(CHAPTER_ID, chapter_id).getSingle();
			if(chapter_node == null)
				throw new KahaniaCustomException("Chapter doesnot exists with given id : "+chapter_id);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			if(user_node == null)
				throw new KahaniaCustomException("User doesnot exists with given id : "+user_id);
			
			if(isRelationExistsBetween(USER_RATED_A_CHAPTER, user_node, chapter_node))
				deleteRelation(USER_RATED_A_CHAPTER, user_node, chapter_node);
			else
			{
				createRelation(USER_RATED_A_CHAPTER, user_node, chapter_node, time).setProperty(CHAPTER_RATING, rating);
			}			
			res = "true";
			tx.success();

		}
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
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
			aquireWriteLock(tx);
			Index<Node> chapterId_index = graphDb.index().forNodes(CHAPTER_ID_INDEX);
			Index<Node> userId_index = graphDb.index().forNodes(USER_ID_INDEX);
			Index<Relationship> userViewedChapterRelIndex = graphDb.index().forRelationships(USER_VIEWED_CHAPTER_REL_INDEX);
			
			Node chapter_node = chapterId_index.get(CHAPTER_ID, chapter_id).getSingle();
			if(chapter_node == null)
				throw new KahaniaCustomException("Chapter doesnot exists with given id : "+chapter_id);
			
			Node user_node = userId_index.get(USER_ID, user_id).getSingle();
			if(user_node == null)
				throw new KahaniaCustomException("User doesnot exists with given id : "+user_id);
			
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
		catch(KahaniaCustomException ex)
		{
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
//				ex.printStackTrace();
			res = "false";
		}
		catch(Exception ex)
		{
			System.out.println("Exception @ favourite_chapter()");
			System.out.println("Something went wrong, while favourite a chapter from favourite_chapter  :"+ex.getMessage());
			ex.printStackTrace();
			res = "false";
		}
		return res;
	}

	@Override
	public String get_feed(String titleType, String feedType, String filter,
			int prev_cnt, int count, String user_id) throws TException {
		// TODO Auto-generated method stub
		return null;
	}
	

}
