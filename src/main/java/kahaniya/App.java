package kahaniya;

public class App 
{
    public static void main( String[] args )
    {
		System.out.println("30th March, 2016 - 12:30 P.M");
		
		Kahaniya kahania = new Kahaniya();
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
