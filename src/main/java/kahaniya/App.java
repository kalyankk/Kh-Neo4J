package kahaniya;

public class App 
{
    public static void main( String[] args )
    {
		System.out.println("15th April, 2016 - 05:00 P.M");
		
		Kahaniya kahaniya = new Kahaniya();
		kahaniya.add_neo4j_lock_nodes();
		kahaniya.startThriftServer();

		//test(kahaniya);

    }
    
    public static void test( Kahaniya k)
    {
    	try{
    		//k.create_genre("GEN1");
    		//k.create_genre("GEN2");
    		//k.create_language("LANG1");
    		//k.create_language("LANG2");
    		//k.create_user("one", "one", "one", "one", "one", "one", "GEN1,GEN2", "LANG1,LANG2", 2, 2, 100);
    		//k.create_user("two", "two", "two", "two", "two", "two", "GEN1,GEN2", "LANG1,LANG2", 2, 2, 100);
    		//k.create_or_edit_series("s1", "one", "s1", "s1", "s1", "feature_image", "GEN1", "LANG2", "keywords", "copyrights", "dd_img", "dd_summary", 1, 101, 0);
    		//k.create_or_edit_series("s2", "one", "s2", "s2", "s2", "feature_image", "GEN2", "LANG2", "keywords", "copyrights", "dd_img", "dd_summary", 1, 102, 0);
    		//k.create_or_edit_chapter("c1", "s1", "series_type", "one", "c1", "c1", "feat_image", 102, 0, 0);
    		//System.out.println(k.get_feed("C", "R", "", 0, 10, "two"));
    		//k.favourite_chapter("c1", "s1", "two", 103);
    		//System.out.println(k.get_feed("S", "SUB", "", 0, 10, "one"));
    		//k.subscribe_series("s1", "one", 105);
    		//System.out.println(k.get_feed("S", "SUB", "", 0, 10, "one"));
    		//k.subscribe_series("s1", "one", 105);
    		//System.out.println(k.get_feed("S", "SUB", "", 0, 10, "one"));
    		//System.out.println(k.get_feed("C", "F", "", 0, 10, "two"));
    		//System.out.println(k.get_feed("", "R", "", 0, 5, "one"));
    		//System.out.println(k.get_feed("", "R", "", 1, 5, "one"));
    		//System.out.println(k.get_feed("", "R", "", 2, 5, "one"));
    		//System.out.println(k.get_feed("", "R", "", 0, 5, "two"));
    		//System.out.println(k.get_feed("", "R", "", 1, 5, "two"));
    		//System.out.println(k.get_feed("", "R", "", 2, 5, "two"));
/*    		System.out.println(k.get_feed("C","G", "", 0, 10, "", "GEN1", ""));
    		System.out.println(k.get_feed("C","G", "", 0, 10, "", "GEN2", ""));
    		System.out.println(k.get_feed("C","LNG", "", 0, 10, "", "", "LANG1"));
    		System.out.println(k.get_feed("C","LNG", "", 0, 10, "", "", "LANG2"));
    		System.out.println(k.get_feed("S","G", "", 0, 10, "", "GEN1", ""));
    		System.out.println(k.get_feed("S","G", "", 0, 10, "", "GEN2", ""));
    		System.out.println(k.get_feed("S","LNG", "", 0, 10, "", "", "LANG1"));
    		System.out.println(k.get_feed("S","LNG", "", 0, 10, "", "", "LANG2")); */
    		
    	}catch(Exception te)
    	{
    		System.out.println("TException");
    		te.printStackTrace();
    	}
    	System.exit(0);
    } 
}
