package kahania;

public class App 
{
    public static void main( String[] args )
    {
		System.out.println("16th March, 2016 - 08:00 A.M");
		
		Kahania kahania = new Kahania();
		kahania.add_neo4j_lock_nodes();
		kahania.startThriftServer();

		//test(kahania);

    }
    
/*    public static void test( Kahania k)
    {
    	try{
    	}catch(TException te)
    	{
    		System.out.println("TException");
    		te.printStackTrace();
    	}
    	System.exit(0);
    } */
}
