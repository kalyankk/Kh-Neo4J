package Saddahaq;

public class Fun {

	public static void main(String[] args) {
	    
		System.out.println("23rd April, 2015 - 10:48 P.M");
		
		User_node algos = new User_node();
		algos.add_firstname_index();
		algos.location_store();
		algos.keyword_store();
		algos.add_exclusive_property();
		algos.add_neo4j_lock_nodes();
		algos.sample_test();
		algos.main();

	}
	
}


/*

		try(Transaction tx = graphDb.beginTx())
		{
			call tx.success() or tx.failure()
		}
		catch(Exception ex)
		{
			System.out.println("Something went wrong, while creating comment from create_comment  :"+ex.getMessage());
			ex.printStackTrace();
			ret = false;
		}
		finally{
			
		}


 */




/*

Transaction tx = null;
 
try 
{
	  tx = graphDb.beginTx();
    Index<Node> nodeIndex = graphDb.index().forNodes( "category" );
    String[] categories = {"politics","cinema-and-entertainment","technology","business","health","sports"};
    
    for(String c : categories){
    	Node foundUser = nodeIndex.get( "name", c ).getSingle();
    	if(foundUser != null)
    	{
    		System.out.println("Got the cat");
    		System.out.println("\tname: "+foundUser.getProperty("name").toString());
    		System.out.println("\tpins: "+foundUser.getProperty("pins").toString());
    		System.out.println("\texclusive: "+foundUser.getProperty("exclusive").toString());
    		System.out.println("\tother_pins: "+foundUser.getProperty("other_pins").toString());
    		System.out.println(foundUser.getDegree());
    	}
    	else System.out.println("didnt Got the cat");
    }
    System.out.println("Completed loop");
    tx.success();
}
catch(Exception e){
	System.out.println("Exception");
	if(tx!=null)
		tx.failure();
}
finally{
	System.out.println("finally");
	if(tx!=null)
	tx.finish();
}

System.out.println("Main finish");
graphDb.shutdown(); */
