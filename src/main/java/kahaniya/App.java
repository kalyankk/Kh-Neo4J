package kahaniya;

public class App 
{
    public static void main( String[] args )
    {
		System.out.println("14th May, 2016 - 08:00 A.M");
		
		Kahaniya kahaniya = new Kahaniya();
		kahaniya.add_neo4j_lock_nodes();
		kahaniya.add_additional_properties();
		kahaniya.startThriftServer();

	//	test(kahaniya);

    }
    
    public static void test( Kahaniya k)
    {
    	try{
//    		System.out.println(k.get_all_items("U", 0, 100));
    	//	System.out.println(k.search("e", 0, "", 0, 100));
    		//System.out.println(k.search("e", 1, "", 0, 100));
    		
  //  		System.out.println(k.get_stats());
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
    		//k.create_or_edit_chapter("c3", "s1", "series_type", "one", "c3", "c3", "feat_image", 104, 0, 0);
    		//k.create_or_edit_chapter("c2", "s1", "series_type", "one", "c2", "c2", "feat_image", 103, 0, 0);
    		//k.create_or_edit_chapter("c4", "s1", "series_type", "one", "c4", "c4", "feat_image", 105, 0, 0);
    		
    		//System.out.println(k.get_feed("C", "L", "", 0, 10, "", "", ""));
    		//System.out.println(k.get_feed("A","D", "", 0, 10, "", "", ""));
    		//System.out.println(k.get_feed("S", "R", "", 0, 10, "", "", ""));
    		//System.out.println(k.get_feed("S", "R", "", 0, 10, "one", "", ""));
    		//System.out.println(k.get_feed("S", "R", "", 0, 10, "two", "", ""));
    		//k.create_user("three", "three", "three", "three", "three", "three", "three", "three", "three", "three", "GEN1,GEN2", "LANG1,LANG2", 2, 2, 105);
    		
    	}catch(Exception te)
    	{
    		System.out.println("TException");
    		te.printStackTrace();
    	}
    	System.exit(0);
    } 
}
