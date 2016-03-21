package kahania;

public class App 
{
    public static void main( String[] args )
    {
		System.out.println("21th March, 2016 - 06:00 P.M");
		
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
