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

public class Kahania implements KahaniaService.Iface{

	
	//Index names
	public static final String USER_ID_INDEX = "user_index_by_id";
	public static final String USER_NAME_INDEX = "user_index_by_uname";
	public static final String USER_EMAIL_INDEX = "user_index_by_email";

	//user related keys
	public static final String USER_ID = "user_id";
	public static final String FULL_NAME = "full_name";
	public static final String USER_NAME = "user_name";
	public static final String EMAIL = "email";
	public static final String MOBILE_NUMBER = "mobile_number";
	public static final String DOB = "dob";
	public static final String PRIVILEGE = "privilege";
	public static final String STATUS = "status";
	public static final String TIME_CREATED = "time_created";
	
	public static final String NODE_TYPE = "node_type";
	public static final String USER_NODE = "user_node";

	private static GraphDatabaseService graphDb = null;
	
	//Relationship names
	RelationshipType USER_GENERS = DynamicRelationshipType.withName("USER_GENERS");
	RelationshipType USER_LANGUAGE = DynamicRelationshipType.withName("USER_LANGUAGE");
	RelationshipType USER_FOLLOW_USER = DynamicRelationshipType.withName("USER_FOLLOW_USER");
	
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
		node.setProperty(PRIVILEGE,privilege);
		node.setProperty(STATUS,status);
	    node.setProperty(TIME_CREATED,time_created);
	    node.setProperty(NODE_TYPE, USER_NODE);
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
		srcNode.createRelationshipTo(targetNode, rel).setProperty(TIME_CREATED, new Date().getTime());
	}


	@Override
	public String create_user(String id, String full_name, String user_name,
			String email, String mobile_number, String dob, String geners,
			String languages, int privilege, int status, int time_created)
			throws TException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public String edit_user_basic_info(String id, String full_name,
			String gender, String dob) throws TException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public String edit_user_contact_details(String id, String email,
			String mobile_number) throws TException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public String edit_user_security_details(String id, String user_name)
			throws TException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public String edit_user_languages(String id, String languages)
			throws TException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public String follow_user(String user_id_1, String user_id_2, int time)
			throws TException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public String deactivate_user(String user_id) throws TException {
		// TODO Auto-generated method stub
		return null;
	}
	
}
