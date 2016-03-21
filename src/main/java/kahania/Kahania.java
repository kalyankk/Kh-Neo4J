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
	public static final String SERIES_TITLE_ID_INDEX = "series_index_by_title_id";
	public static final String SERIES_TYPE_INDEX = "series_index_by_type";
	
	public static final String LOCK_INDEX = "lock_index";
	
	//lock related keys
	public static final String LockName = "lock_node";

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
	
	//genre node related keys
	public static final String GENRE_NAME = "genre_name";

	//language node related keys
	public static final String LANG_NAME = "language_name";
	
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

	private static GraphDatabaseService graphDb = null;
	
	//Relationship names
	RelationshipType USER_INTERESTED_GENRE = DynamicRelationshipType.withName("USER_INTERESTED_GENRE");
	RelationshipType USER_INTERESTED_LANGUAGE = DynamicRelationshipType.withName("USER_INTERESTED_LANGUAGE");
	RelationshipType USER_FOLLOW_USER = DynamicRelationshipType.withName("USER_FOLLOW_USER");
	RelationshipType USER_STARTED_SERIES = DynamicRelationshipType.withName("USER_STARTED_SERIES");

	RelationshipType SERIES_BELONGS_TO_GENRE = DynamicRelationshipType.withName("SERIES_BELONGS_TO_GENRE");
	RelationshipType SERIES_BELONGS_TO_LANGUAGE = DynamicRelationshipType.withName("SERIES_BELONGS_TO_LANGUAGE");
	
	
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
									USER_ID_INDEX,
									USER_NAME_INDEX,
									USER_EMAIL_INDEX,
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

	private boolean isRelationExistsBetween(RelationshipType rel, Node srcNode, Node targetNode)
	{
		Iterator<Relationship> itr = srcNode.getRelationships(Direction.OUTGOING, rel).iterator();
		while(itr.hasNext())
		{
			if(targetNode.equals(itr.next().getEndNode())) return true;
		}
		return false;
	}

	private void deleteRelation(RelationshipType rel, Node srcNode, Node targetNode)
	{
		Iterator<Relationship> itr = srcNode.getRelationships(Direction.OUTGOING, rel).iterator();
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

	private void createRelation(RelationshipType rel, Node srcNode, Node targetNode)
	{
		srcNode.createRelationshipTo(targetNode, rel).setProperty(TIME_CREATED, (int)(System.currentTimeMillis()/1000));
	}

	private void createRelation(RelationshipType rel, Node srcNode, Node targetNode, int time_created)
	{
		srcNode.createRelationshipTo(targetNode, rel).setProperty(TIME_CREATED, time_created);
	}

	@Override
	public String create_genre(String name)
			throws TException {
		String res;		
		try(Transaction tx = graphDb.beginTx())
		{
			aquireWriteLock(tx);
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			
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
			
			Node genre_node = genreName_index.get(GENRE_NAME,old_name).getSingle();
			
			if(genre_node == null)
				throw new KahaniaCustomException("Genre node doesnot exists with given name");

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
			
			Node genre_node = genreName_index.get(GENRE_NAME,name).getSingle();
			
			if(genre_node == null)
				throw new KahaniaCustomException("Genre node doesnot exists with given name");

			//Remove relationships for genre node
			for(Relationship rel : genre_node.getRelationships())
				rel.delete();

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
			
			Node lang_node = langName_index.get(LANG_NAME,old_name).getSingle();
			
			if(lang_node == null)
				throw new KahaniaCustomException("Lang node doesnot exists with given name");

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
			
			Node lang_node = langName_index.get(LANG_NAME,name).getSingle();
			
			if(lang_node == null)
				throw new KahaniaCustomException("Lang node doesnot exists with given name");

			//Remove relationships for lang node
			for(Relationship rel : lang_node.getRelationships())
				rel.delete();

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
				if(genreName_index.get(GENRE_NAME, genre_name).getSingle() != null)
					createRelation(USER_INTERESTED_GENRE, user_node, genreName_index.get(GENRE_NAME, genre_name).getSingle());
			}
			
			for(String lang_name : languages.split(","))
			{
				if(langName_index.get(LANG_NAME, lang_name).getSingle() != null)
					createRelation(USER_INTERESTED_LANGUAGE, user_node, langName_index.get(LANG_NAME, lang_name).getSingle());
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
				if(langName_index.get(LANG_NAME, lang_name).getSingle() != null)
					createRelation(USER_INTERESTED_LANGUAGE, user_node, langName_index.get(LANG_NAME, lang_name).getSingle());
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
				if(genreName_index.get(GENRE_NAME, lang_name).getSingle() != null)
					createRelation(USER_INTERESTED_GENRE, user_node, genreName_index.get(GENRE_NAME, lang_name).getSingle());
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
			
			Node userNode = userId_index.get(USER_ID,user_id).getSingle();
			if(userNode == null)
				throw new KahaniaCustomException("User doesnot exists with given id : "+user_id);
			if(seriesId_index.get(SERIES_ID,series_id).getSingle()!=null)
				throw new KahaniaCustomException("Series already exists with given id : "+series_id);
			if(seriesTitleId_index.get(SERIES_TITLE_ID,title_id).getSingle()!=null)
				throw new KahaniaCustomException("Series already exists with given title id : "+title_id);

			Node series_node = Series(series_id, title, title_id, tag_line, feature_image, keywords, copyrights, dd_img, dd_summary, series_type, time_created);  // Creating a new series node
			if(series_node == null)
				throw new KahaniaCustomException("Something went wrong, while creating series ");

			//create relationship with user
			
			createRelation(USER_STARTED_SERIES, userNode, series_node);
			
			//create relationships with Genres and Languages
			for(String genre_name : genre.split(","))
			{
				if(genreName_index.get(GENRE_NAME, genre_name).getSingle() != null)
					createRelation(SERIES_BELONGS_TO_GENRE, series_node, genreName_index.get(GENRE_NAME, genre_name).getSingle());
			}
			
			for(String lang_name : language.split(","))
			{
				if(langName_index.get(LANG_NAME, lang_name).getSingle() != null)
					createRelation(SERIES_BELONGS_TO_LANGUAGE, series_node, langName_index.get(LANG_NAME, lang_name).getSingle());
			}
			
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
			Index<Node> seriesTitleId_index = graphDb.index().forNodes(SERIES_TITLE_ID_INDEX);
			Index<Node> seriesType_index = graphDb.index().forNodes(SERIES_TYPE_INDEX);
			
			Index<Node> genreName_index = graphDb.index().forNodes(GENRE_NAME_INDEX);
			Index<Node> langName_index = graphDb.index().forNodes(LANG_NAME_INDEX);
			
			Node series_node = seriesId_index.get(SERIES_ID, series_id).getSingle();
			if(series_node == null)
				throw new KahaniaCustomException("Series doesnot exists with given id : "+series_id);
			if(seriesTitleId_index.get(SERIES_TITLE_ID,title_id).getSingle()!=null)
				throw new KahaniaCustomException("Series already exists with given title id : "+title_id);

			//remove existing relationships with Genres and Languages
			for(Relationship rel : series_node.getRelationships(SERIES_BELONGS_TO_GENRE))
				rel.delete();
			for(Relationship rel : series_node.getRelationships(SERIES_BELONGS_TO_LANGUAGE))
				rel.delete();

			series_node.setProperty(SERIES_TITLE, title);
			series_node.setProperty(SERIES_TITLE_ID, title_id);
			series_node.setProperty(SERIES_TAG_LINE, tag_line);
			series_node.setProperty(SERIES_FEAT_IMG, feature_image);
			series_node.setProperty(SERIES_KEYWORDS, keywords);
			series_node.setProperty(SERIES_COPYRIGHTS, copyrights);
			series_node.setProperty(SERIES_DD_IMG, dd_img);
			series_node.setProperty(SERIES_DD_SUMMARY, dd_summary);
			series_node.setProperty(SERIES_TYPE, series_type);
			
			//create relationships with Genres and Languages
			for(String genre_name : genre.split(","))
			{
				if(genreName_index.get(GENRE_NAME, genre_name).getSingle() != null)
					createRelation(SERIES_BELONGS_TO_GENRE, series_node, genreName_index.get(GENRE_NAME, genre_name).getSingle());
			}
			
			for(String lang_name : language.split(","))
			{
				if(langName_index.get(LANG_NAME, lang_name).getSingle() != null)
					createRelation(SERIES_BELONGS_TO_LANGUAGE, series_node, langName_index.get(LANG_NAME, lang_name).getSingle());
			}
			
			//update indexing for the edited series node
			seriesTitleId_index.remove(series_node);
			seriesTitleId_index.add(series_node, SERIES_TITLE_ID, title_id);
			seriesType_index.remove(series_node);
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
	

}
