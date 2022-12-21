import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.io.Files;

  
public class knn
{static int i;
static int accuracy;
static int prec;
static int K;
  public static class DoubleInterger implements WritableComparable<DoubleInterger>
  {
    private Double distance=0.0;
    private Integer classic=-1;

    public void set(Double dis, Integer classy)
    {
      distance=dis;
      classic=classy;
    }

    public Double getDistance()
    {
      return distance;
    }
    public Integer getclass()
    {
      return classic;
    }
    @Override
    public void readFields(DataInput in) throws IOException
    {
      distance = in.readDouble();
      classic = in.readInt();
    }
    @Override
    public void write(DataOutput out) throws IOException
		{
				out.writeDouble(distance);
				out.writeInt(classic);
		}
    @Override
			public int compareTo(DoubleInterger o)
			{
				return (this.classic).compareTo(o.classic);
			}
  }
  public static class knnMapper extends Mapper< Object, Text, NullWritable,DoubleInterger>
  {
    DoubleInterger disandclass= new DoubleInterger();
    TreeMap<Double, Integer> tree = new TreeMap<Double,Integer>();

    
    double normalisedattribute1;
    double normalisedattribute2;
    double normalisedattribute3;
    double normalisedattribute4;
    double normalisedattribute5;
    double normalisedattribute6;
    double normalisedattribute7;
    double normalisedattribute8;
    double normalisedattribute9;

    private double normalisedDouble(String n1)
		{
			return Double.parseDouble(n1) ;
		}
    private double squaredDistance(double n1)
		{
			return Math.pow(n1,2);
		}

    private double tot_dis(double R1,double R2, double R3, double R4,double R5,double R6,double R7,double R8,double R9,double S1,double S2,double S3,double S4,double S5,double S6,double S7,double S8,double S9)
    {
    double dif1=squaredDistance( R1-S1);
    double dif2=squaredDistance( R2-S2);
    double dif3=squaredDistance( R3-S3);
    double dif4=squaredDistance( R4-S4);
    double dif5=squaredDistance( R5-S5);
    double dif6=squaredDistance( R6-S6);
    double dif7=squaredDistance( R7-S7);
    double dif8=squaredDistance( R8-S8);
    double dif9=squaredDistance( R9-S9);
    return dif1+dif2+dif3+dif4+dif5+dif6+dif7+dif8+dif9;
    }
    protected void setup(Context context) throws IOException, InterruptedException
		{
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0)
			{
				// Read parameter file using alias established in main()
				String knnParams = FileUtils.readFileToString(new File("./knnParamFile"));
				StringTokenizer st1 = new StringTokenizer(knnParams, "\n");
				if(i==0)
				{K = Integer.parseInt(st1.nextToken());
				i=1;
				}
				for(int j=0;j<=i;j++)
					st1.nextToken();
				//out.println("%d",i);
		    	// Using the variables declared earlier, values are assigned to K and to the test dataset, S.
		    	// These values will remain unchanged throughout the mapper
				StringTokenizer st = new StringTokenizer(st1.nextToken(), " ");
				
				normalisedattribute1= normalisedDouble(st.nextToken());
				normalisedattribute2= normalisedDouble(st.nextToken());
        normalisedattribute3= normalisedDouble(st.nextToken());
        normalisedattribute4= normalisedDouble(st.nextToken());
        normalisedattribute5= normalisedDouble(st.nextToken());
        normalisedattribute6= normalisedDouble(st.nextToken());
        normalisedattribute7= normalisedDouble(st.nextToken());
        normalisedattribute8= normalisedDouble(st.nextToken());
        normalisedattribute9= normalisedDouble(st.nextToken());
        prec=Integer.parseInt(st.nextToken());
        //System.out.println(""+prec);
			}
		}

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{

			String rLine = value.toString();
			StringTokenizer st = new StringTokenizer(rLine, " ");
		//	System.out.println(":::::::::::::::::::::::::::::::::::::::::::;");
      double rnormalisedattribute1= normalisedDouble(st.nextToken());
      double rnormalisedattribute2= normalisedDouble(st.nextToken());
      double rnormalisedattribute3= normalisedDouble(st.nextToken());
      double rnormalisedattribute4= normalisedDouble(st.nextToken());
      double rnormalisedattribute5= normalisedDouble(st.nextToken());
      double rnormalisedattribute6= normalisedDouble(st.nextToken());
      double rnormalisedattribute7= normalisedDouble(st.nextToken());
      double rnormalisedattribute8= normalisedDouble(st.nextToken());
      double rnormalisedattribute9= normalisedDouble(st.nextToken());
			int rclass=Integer.parseInt(st.nextToken());

			double tDist = tot_dis(normalisedattribute1,normalisedattribute2,normalisedattribute3,normalisedattribute4,normalisedattribute5,normalisedattribute6,normalisedattribute7,normalisedattribute8,normalisedattribute9,rnormalisedattribute1,rnormalisedattribute2,rnormalisedattribute3,rnormalisedattribute4,rnormalisedattribute5,rnormalisedattribute6,rnormalisedattribute7,rnormalisedattribute8,rnormalisedattribute9);


			tree.put(tDist, rclass);

			if (tree.size() > K)
			{
				tree.remove(tree.lastKey());
			}
		}
    protected void cleanup(Context context) throws IOException, InterruptedException
    		{
    			// Loop through the K key:values in the TreeMap
    			for(Map.Entry<Double, Integer> entry : tree.entrySet())
    			{
    				  Double knnDist = entry.getKey();
    				  Integer knnModel = entry.getValue();
    				  // distanceAndModel is the instance of DoubleString declared earlier
    				  disandclass.set(knnDist, knnModel);
    				  // Write to context a NullWritable as key and distanceAndModel as value
    				  context.write(NullWritable.get(), disandclass);
    			}
    		}


  }
  public static class KnnReducer extends Reducer<NullWritable, DoubleInterger, NullWritable, Text>
	{
		TreeMap<Double, Integer> KnnMap = new TreeMap<Double, Integer>();
		//int K;

		@Override
		// setup() again is run before the main reduce() method
		protected void setup(Context context) throws IOException, InterruptedException
		{//System.out.println(":::::::::::::::::::::::::::::::::::::::::::;");
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0)
			{
			
			}
		}

		
		@Override
		// The reduce() method accepts the objects the mapper wrote to context: a NullWritable and a DoubleString
		public void reduce(NullWritable key, Iterable<DoubleInterger> values, Context context) throws IOException, InterruptedException
		{	
			// values are the K DoubleString objects which the mapper wrote to context
			// Loop through these
			for (DoubleInterger val : values)
			{
				Integer rModel = val.getclass();
				double tDist = val.getDistance();

				// Populate another TreeMap with the distance and model information extracted from the
				// DoubleString objects and trim it to size K as before.
				KnnMap.put(tDist, rModel);
				if (KnnMap.size() > K)
				{
					KnnMap.remove(KnnMap.lastKey());
				}
			}

				
				List<Integer> knnList = new ArrayList<Integer>(KnnMap.values());

				Map<Integer, Integer> freqMap = new HashMap<Integer, Integer>();

			    // Add the members of the list to the HashMap as keys and the number of times each occurs
			    // (frequency) as values
			    for(int i=0; i< knnList.size(); i++)
			    {
			        Integer frequency = freqMap.get(knnList.get(i));
			        if(frequency == null)
			        {
			            freqMap.put(knnList.get(i), 1);
			        } else
			        {
			            freqMap.put(knnList.get(i), frequency+1);
			        }
			    }

			    Integer mostCommonModel = 0;
			    int maxFrequency = -1;
			    for(Map.Entry<Integer, Integer> entry: freqMap.entrySet())
			    {
			        if(entry.getValue() > maxFrequency)
			        {
			            mostCommonModel = entry.getKey();
			            maxFrequency = entry.getValue();
			        }
			    }
			   
			    //System.out.println(""+mostCommonModel);
			   if(mostCommonModel==prec)
			    	accuracy++;
			
			context.write(NullWritable.get(), new Text(mostCommonModel.toString())); 

		}
	}
  public static void main(String[] args) throws Exception
	{
		// Create configuration
		Configuration conf = new Configuration();

		if (args.length != 3)
		{
			System.err.println("Usage: KnnPattern <in> <out> <parameter file>");
			System.exit(2);
		}
//		
     int linecount=0;            //Intializing linecount as zero
//	      FileReader fr=new FileReader(f1);  //Creation of File Reader object
//	      BufferedReader br = new BufferedReader(fr);    //Creation of File Reader object
//	      String s;              
//	      while((s=br.readLine())!=null)    //Reading Content from the file line by line
//	      {
//	         linecount++;               //For each line increment linecount by one 
//	            
//	      }
//	      fr.close();
		accuracy=0;
		long startTime = System.currentTimeMillis();
		Path inputPath = new Path(args[2]);
		//Configuration conf1 = getConf();
		FileSystem fs = FileSystem.get(conf);
        InputStream is = fs.open(inputPath);
        InputStreamReader isr = new InputStreamReader(is,
                StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr);    //Creation of File Reader object
	      String s;              
	      if((s=br.readLine())!=null)    //Reading Content from the file line by line
	      {
	         linecount=Integer.parseInt(s);              //For each line increment linecount by one 
	      }   
	      
	      //fs.close();
	      
	      //fr.close();
//		java.nio.file.Path path = Paths.get(args[2]);
//		long linecount = Files.lines(path).count();
	  int up=0;
	      for (i = 0; i<linecount; ++i){
		// Create job
	    	  up++;
		Job job = Job.getInstance(conf, "Find K-Nearest Neighbour");
		job.setJarByClass(knn.class);
		// Set the third parameter when running the job to be the parameter file and give it an alias
		job.addCacheFile(new URI(args[2] + "#knnParamFile")); // Parameter file containing test data


		job.setMapperClass(knnMapper.class);
		job.setReducerClass(KnnReducer.class);
		job.setNumReduceTasks(1); // Only one reducer in this design

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DoubleInterger.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//if(i==0)
		fs.delete(new Path(args[1]+up),true);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]+up));
		
		
		job.waitForCompletion(true) ;
		job.close();
		
		
	    }
	      long endTime = System.currentTimeMillis();
	     // System.out.println("Value of i"+i);
	      double d=accuracy;
	      double dab=d/(i-1);
	     System.out.println("Accuracy="+dab*100+"%");
	      System.out.println("Time:"+(endTime - startTime)/1000 + " s"); 


	}
}
