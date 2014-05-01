package package1;



public class Run {
	public static void main(String[] args) throws Exception 
	{
		//TestRun.run(args);
		
		if(TestRun.run(args)==0)
		{
			if(TestRun2.run(args)==0)
			{
				TestRun4.run(args);
			}
		}
	}
	
}
