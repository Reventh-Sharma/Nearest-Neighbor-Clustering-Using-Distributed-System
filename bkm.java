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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeMap;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.io.Files;



//import knnassignment.DoubleInterger;


public class bkm{
	public static ArrayList<pt> centroids;
	static int K=7;
	static int u=2;
	static int q=0;
	static Long change;
	static ArrayList<Long>pa;
	//int k11,k12,k13,k14,k15,k16,k17,k18,k19,k110,k111,k112,k113;
	//int k21,k22,k23,k24,k25,k26,k27,k28,k29,k210,k211,k212,k213;
	static HashMap<Long, ArrayList<Double>> map = new HashMap<>(); 
	static HashMap<Long, ArrayList<Double>> centroid= new HashMap<>(); 
	static HashMap<Long,ArrayList<Long>> nodes=new HashMap<>();
	static HashMap<Long,Long> numbers=new HashMap<>();
	//static TreeMap<Long,Integer> ccc=new TreeMap<>();
	static long i;
	public static long x1=0,x2=1;
	static pt a1;
	static int john=0;
	static pt a2;
	static int number=5;
	static ArrayList<Double>sse= new ArrayList<Double>();
	static int check=0;
	public static class pt implements WritableComparable<pt>{
		double k11,k12,k13,k14,k15,k16,k17,k18,k19,k110,k111,k112,k113;
		public pt(double arg1,double arg2,double arg3,double arg4, double arg5,double arg6,double arg7,double arg8,double arg9,double arg10,double arg11,double arg12,double arg13)
		{
			k11=arg1;
			k12=arg2;
			k13=arg3;
			k14=arg4;
			k15=arg5;
			k16=arg6;
			k17=arg7;
			k18=arg8;
			k19=arg9;
			k110=arg10;
			k111=arg11;
			k112=arg12;
			k113=arg13;
			
		}
		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			
			k11=arg0.readInt();
			k12=arg0.readInt();
			k13=arg0.readInt();
			k14=arg0.readInt();
			k15=arg0.readInt();
			k16=arg0.readInt();
			k17=arg0.readInt();
			k18=arg0.readInt();
			k19=arg0.readInt();
			k110=arg0.readInt();
			k111=arg0.readInt();
			k112=arg0.readInt();
			k113=arg0.readInt();
			
		}
		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			arg0.writeDouble(k11);
			arg0.writeDouble(k12);
			arg0.writeDouble(k13);
			arg0.writeDouble(k14);
			arg0.writeDouble(k15);
			arg0.writeDouble(k16);
			arg0.writeDouble(k17);
			arg0.writeDouble(k18);
			arg0.writeDouble(k19);
			arg0.writeDouble(k110);
			arg0.writeDouble(k111);
			arg0.writeDouble(k112);
			arg0.writeDouble(k113);
		}
		@Override
		public int compareTo(pt arg0) {
			// TODO Auto-generated method stub
			return 0;
		}
	
	}
	
	
	
	public static class kmeansMapper extends Mapper< Object, Text, LongWritable,LongWritable>
	  {		
	  		//long x1,x2;
	  TreeMap<pt, ArrayList<pt>> tree = new TreeMap<pt,ArrayList<pt>>();
	  TreeMap<Long,ArrayList<Long>> t1=new TreeMap<Long,ArrayList<Long>>();
	    protected void setup(Context context) throws IOException, InterruptedException
			{// have to set up k1 and k2 somehow
				
	    	i=5;
	    	ArrayList<pt> c1=new ArrayList<pt>();
	    	ArrayList<pt> c2=new ArrayList<pt>();
	    	tree.put(a1, c1);
	    	tree.put(a2, c2);
	    	ArrayList<Long> ar1=new ArrayList<Long>();
	    	ArrayList<Long> ar2=new ArrayList<Long>();
	    	t1.put(x1, ar1);
	    	t1.put(x2, ar2);
			}
	    private double squaredDistance(double n1)
		{
			return Math.pow(n1,2);
		}
		public double distance(pt a1,pt a2)
		{
			double dif1=squaredDistance( a1.k11-a2.k11);
		    double dif2=squaredDistance( a1.k12-a2.k12);
		    double dif3=squaredDistance( a1.k13-a2.k13);
		    double dif4=squaredDistance( a1.k14-a2.k14);
		    double dif5=squaredDistance( a1.k15-a2.k15);
		    double dif6=squaredDistance( a1.k16-a2.k16);
		    double dif7=squaredDistance( a1.k17-a2.k17);
		    double dif8=squaredDistance( a1.k18-a2.k18);
		    double dif9=squaredDistance(a1.k19-a2.k19 );
		    double dif10=squaredDistance( a1.k110-a2.k110);
		    double dif11=squaredDistance( a1.k111-a2.k111);
		    double dif12=squaredDistance( a1.k112-a2.k112);
		    double dif13=squaredDistance( a1.k113-a2.k113);
		    return dif1+dif2+dif3+dif4+dif5+dif6+dif7+dif8+dif9+dif10+dif11+dif12+dif13;
		}
		public pt getcloser(pt a1,pt a2,pt a3)
		{	double a=distance(a1,a3);
			double b=distance(a2,a3);
			if(a<b)
				return a1;
			else 
				return a2;
			
		}
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
			{
	    	if(check==0)
	    	{
	    	String rLine = value.toString();
			StringTokenizer st = new StringTokenizer(rLine, "\t");
			double  arg1=Double.parseDouble(st.nextToken());
			double  arg2=Double.parseDouble(st.nextToken());
			double arg3=Double.parseDouble(st.nextToken());
			double  arg4=Double.parseDouble(st.nextToken());
			double  arg5=Double.parseDouble(st.nextToken());
			double  arg6=Double.parseDouble(st.nextToken());
			double  arg7=Double.parseDouble(st.nextToken());
			double arg8=Double.parseDouble(st.nextToken());
			double  arg9=Double.parseDouble(st.nextToken());
			double  arg10=Double.parseDouble(st.nextToken());
			double  arg11=Double.parseDouble(st.nextToken());
			double arg12=Double.parseDouble(st.nextToken());
			double arg13=Double.parseDouble(st.nextToken());
			
			pt a3=new pt(arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8,arg9,arg10,arg11,arg12,arg13);
			//System.out.println("++++++++++++++++++++++++++++++++++++");
			pt b1=getcloser(a1,a2,a3);
			ArrayList<pt> t=tree.get(b1);
		
			tree.remove(b1);
			t.add(a3);
			tree.put(b1, t);
			ArrayList<Double> a=new ArrayList<>();
			a.add(arg1);
			a.add(arg2);
			a.add(arg3);
			a.add(arg4);
			a.add(arg5);
			a.add(arg6);
			a.add(arg7);
			a.add(arg8);
			a.add(arg9);
			a.add(arg10);
			a.add(arg11);
			a.add(arg12);
			a.add(arg13);
			if(!map.containsKey(i))
			map.put(i,a);
			ArrayList<Long> g;
			if(b1==a1)
			{
				g=t1.get(x1);
				t1.remove(x1);
				g.add(i);
				t1.put(x1, g);
			}
			else
			{
				g=t1.get(x2);
				t1.remove(x2);
				g.add(i);
				t1.put(x2, g);
			
			}
			i++;}
	    	else if(q<john)
	    	{	
	    		
	    		//if(pa.size()>2312)
	    		//System.out.println("=========================="+pa.get(2320)+"============"+pa.size());
	    		
	    		Long d=pa.get(q);
	    		
	    		q++;
	    		Long v1=dis(x1,x2,d);
	    		ArrayList<Long> g;
	    		
				if(v1==x1)
				{
					g=t1.get(x1);
					t1.remove(x1);
					g.add(d);
					t1.put(x1, g);
				}
				else
				{
					g=t1.get(x2);
					t1.remove(x2);
					g.add(d);
					t1.put(x2, g);
					
				}
	    	}
			
			
			}
	    protected void cleanup(Context context) throws IOException, InterruptedException
	    		{
	    		for(Map.Entry<Long,ArrayList<Long>> entry : t1.entrySet())
    			{	Long l=entry.getKey();
    				ArrayList<Long> o=entry.getValue();
    				for(Long g:o) {
	    	context.write(new LongWritable(l),new LongWritable(g));
	    	//System.out.println("_________________________"+g);
    				}
    			}
	    		
	    		}
	   
	    public long dis(long x1,long x2,long x3)
	    { 	ArrayList<Double> fa,fa2,ga;
	    fa=centroid.get(x1);
	    fa2=centroid.get(x2);
	    ga=map.get(x3);
	    
	    	double a= tot_dis(fa.get(0),fa.get(1),fa.get(2),fa.get(3),fa.get(4),fa.get(5),fa.get(6),fa.get(7),fa.get(8),fa.get(9),fa.get(10),fa.get(11),fa.get(12),ga.get(0),ga.get(1),ga.get(2),ga.get(3),ga.get(4),ga.get(5),ga.get(6),ga.get(7),ga.get(8),ga.get(9),ga.get(10),ga.get(11),ga.get(12));
	    	double b= tot_dis(fa2.get(0),fa2.get(1),fa2.get(2),fa2.get(3),fa2.get(4),fa2.get(5),fa2.get(6),fa2.get(7),fa2.get(8),fa2.get(9),fa2.get(10),fa2.get(11),fa2.get(12),ga.get(0),ga.get(1),ga.get(2),ga.get(3),ga.get(4),ga.get(5),ga.get(6),ga.get(7),ga.get(8),ga.get(9),ga.get(10),ga.get(11),ga.get(12));
	    if(a>b)
	    	return x1;
	    else
	    	return x2;
	    }
	    
   public double tot_dis(double R1,double R2, double R3, double R4,double R5,double R6,double R7,double R8,double R9,double R10,double R11,double R12,double R13,double S1,double S2,double S3,double S4,double S5,double S6,double S7,double S8,double S9, double S10,double S11,double S12,double S13)
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
    double dif10=squaredDistance( R10-S10);
    double dif11=squaredDistance( R11-S11);
    double dif12=squaredDistance( R12-S12);
    double dif13=squaredDistance( R13-S13);
    return dif1+dif2+dif3+dif4+dif5+dif6+dif7+dif8+dif9+dif10+dif11+dif12+dif13;
    }


	  }
	public static class KmeansReducer extends Reducer<LongWritable,LongWritable, LongWritable, Text>
	{
		
		
		@Override
		// setup() again is run before the main reduce() method
		protected void setup(Context context) throws IOException, InterruptedException
		{
		}

		
		@Override
		// The reduce() method accepts the objects the mapper wrote to context: a NullWritable and a DoubleString
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{	double k21 = 0,k22 = 0,k23 = 0,k24 = 0,k25 = 0,k26 = 0,k27 = 0,k28 = 0,k29 = 0,k210 = 0,k211 = 0,k212 = 0,k213=0;
		int x=0;
		ArrayList<Long> al=new ArrayList<>();
		
			// values are the K DoubleString objects which the mapper wrote to context
			// Loop through these
		if(nodes.containsKey(key.get()))
			nodes.remove(key.get());
			for (LongWritable val : values)
			{x++;
//				Integer rModel = val.getclass();
//				double tDist = val.getDistance();
				long g=val.get();
				al.add(g);
				ArrayList<Double>p=map.get(g);
				ArrayList<Long> t;
				
				if(nodes.containsKey(key.get()))
				{t=nodes.get(key.get());
				t.add(g);
				//nodes.replace(key.get(),t);
				nodes.remove(key.get());
				nodes.put(key.get(),t);
				//System.out.println("sad");
				}
				else
				{
				t=new ArrayList<>();
				t.add(g);
				nodes.put(key.get(), t);
				}
				k21=k21+p.get(0);
				k22+=p.get(1);
				k23+=p.get(2);
				k24+=p.get(3);
				k25+=p.get(4);
				k26+=p.get(5);
				k27+=p.get(6);
				k28+=p.get(7);
				k29+=p.get(8);
				k210+=p.get(9);
				k211+=p.get(10);
				k212+=p.get(11);
				k213+=p.get(12);
			
			}
			ArrayList<Double> abc=new ArrayList<Double>();
			abc.add(k21/x);
			abc.add(k22/x);
			abc.add(k23/x);
			abc.add(k24/x);
			abc.add(k25/x);
			abc.add(k26/x);
			abc.add(k27/x);
			abc.add(k28/x);
			abc.add(k29/x);
			abc.add(k210/x);
			abc.add(k211/x);
			abc.add(k212/x);
			abc.add(k213/x);
			
			Double v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13;
			v1=k21/x;
			v2=k22/x;
			v3=k23/x;
			v4=k24/x;
			v5=k25/x;
			v6=k26/x;
			v7=k27/x;
			v8=k28/x;
			v9=k29/x;
			v10=k210/x;
			v11=k211/x;
			v12=k212/x;
			v13=k213/x;
			long h=key.get();
			map.replace(h, abc);
			centroid.replace(h, abc);
			double dist1 = 0;
			int o=0;
			for(long j:al)
			{//dist1++;
			o++;
//				Integer rModel = val.getclass();
//				double tDist = val.getDistance();
				
				ArrayList<Double>p=map.get(j);
				ArrayList<Double>f=centroid.get(h);
				dist1=dist1+tot_dis(p.get(0),p.get(1),p.get(2),p.get(3),p.get(4),p.get(5),p.get(6),p.get(7),p.get(8),p.get(9),p.get(10),p.get(11),p.get(12),f.get(0),f.get(1),f.get(2),f.get(3),f.get(4),f.get(5),f.get(6),f.get(7),f.get(8),f.get(9),f.get(10),f.get(11),f.get(12));
				
				
				
			}
			sse.remove((int)h);
			sse.add((int) h, dist1);
			if(numbers.containsKey(h))
				numbers.replace(h, (long) o);
				
			else
				numbers.put(h,(long) o);
			
			//System.out.println(h+"--------"+sse.get((int)(h))+"---q---"+q+"--------"+o+"-------------"+v1+" "+v2+" "+v3+" "+v4+" "+v5+" "+v6+" "+v7+" "+v8+" "+v9+" "+v10+" "+v11+" "+v12+" "+v13);
			context.write(new LongWritable(h),new Text(v1.toString()+v2.toString()));
			
		}
		
		private double squaredDistance(double n1)
		{
			return Math.pow(n1,2);
		}

   public double tot_dis(double R1,double R2, double R3, double R4,double R5,double R6,double R7,double R8,double R9,double R10,double R11,double R12,double R13,double S1,double S2,double S3,double S4,double S5,double S6,double S7,double S8,double S9, double S10,double S11,double S12,double S13)
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
    double dif10=squaredDistance( R10-S10);
    double dif11=squaredDistance( R11-S11);
    double dif12=squaredDistance( R12-S12);
    double dif13=squaredDistance( R13-S13);
    return dif1+dif2+dif3+dif4+dif5+dif6+dif7+dif8+dif9+dif10+dif11+dif12+dif13;
    }
	}
	public static void main(String[] args) throws Exception
	{
	Configuration conf = new Configuration();

	if (args.length != 2)
	{
		System.err.println("Usage: KnnPattern <in> <out> <parameter file>");
		System.exit(2);
	}
//	
	for(int i=0;i<K;i++)
	{	
		sse.add(0.00);
	}
	long startTime = System.currentTimeMillis();
	//Path inputPath = new Path(args[2]);
	
      //fs.close();
      
      //fr.close();
//	java.nio.file.Path path = Paths.get(args[2]);
//	long linecount = Files.lines(path).count();
	
	for(int y=0;y<K-1;y++)
	{
	if(check==1)
	if(x1<5&&x2<5&&sse.get((int) x1)>sse.get((int) x2))
	{	change =(long) x1;
	//System.out.println("?????????????????????"+x2);
		if(x1>x2)
			x2=x1+1;
		else
			x2=x2+1;
		pa=new ArrayList<Long>(nodes.get(change));
		
	}
	else if(x1<5&&x2<5)
	{change =(long) x2;
	//System.out.println("?????????????????????"+x1);
		if(x1>x2)
		x1=x1+1;
	else
		x1=x2+1;
		pa=new ArrayList<Long>(nodes.get(change));
	}
	
	u++;
	sse.set((int) x1, 0.0);
	sse.set((int) x2, 0.0);
	numbers.replace(x1,(long) 0);
	numbers.replace(x2,(long) 0);
	Random rand = new Random(); 
	
	if(check==0) {
	double d1=rand.nextInt(1000)+rand.nextDouble(); 
  		
  double d2=rand.nextInt(10000)+rand.nextDouble(); 
  double d3=rand.nextInt(50)+rand.nextDouble(); 
  double d4=rand.nextInt(500)+rand.nextDouble(); 
  double d5=rand.nextInt(50)+rand.nextDouble(); 
  double d6=rand.nextInt(50)+rand.nextDouble(); 
  double d7=rand.nextInt(10)+rand.nextDouble(); 
  double d8=rand.nextInt(10000)+rand.nextDouble(); 
  double d9=rand.nextInt(200)+rand.nextDouble(); 
  double d10=rand.nextInt(2)+rand.nextDouble(); 
  double d11=rand.nextInt(50)+rand.nextDouble(); 
  double d12=rand.nextInt(100000)+rand.nextDouble(); 
  double d13=rand.nextInt(100)+rand.nextDouble();     
  
  a1=new pt(d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13);
  ArrayList<Double> a=new ArrayList<>();
	a.add(d1);
	a.add(d2);
	a.add(d3);
	a.add(d4);
	a.add(d5);
	a.add(d6);
	a.add(d7);
	a.add(d8);
	a.add(d9);
	a.add(d10);
	a.add(d11);
	a.add(d12);
	a.add(d13);
	if(!map.containsKey(x1))
	map.put(x1,a);
	else
		map.replace(x1, a);
	if(!centroid.containsKey(x1))
	centroid.put(x1, a);
	else
		centroid.replace(x1, a);
	
	  double c1=rand.nextInt(1000)+rand.nextDouble(); 
	  double c2=rand.nextInt(10000)+rand.nextDouble(); 
	  double c3=rand.nextInt(50)+rand.nextDouble(); 
	  double c4=rand.nextInt(500)+rand.nextDouble(); 
	  double c5=rand.nextInt(50)+rand.nextDouble(); 
	  double c6=rand.nextInt(50)+rand.nextDouble(); 
	  double c7=rand.nextInt(10)+rand.nextDouble(); 
	  double c8=rand.nextInt(10000)+rand.nextDouble(); 
	  double c9=rand.nextInt(200)+rand.nextDouble(); 
	  double c10=rand.nextInt(2)+rand.nextDouble(); 
	  double c11=rand.nextInt(50)+rand.nextDouble(); 
	  double c12=rand.nextInt(100000)+rand.nextDouble(); 
	  double c13=rand.nextInt(100)+rand.nextDouble();       
	  //System.out.println(""+d1+" "+d2+" "+d3+" "+d4+" "+d5+" "+d6+" "+d7+" "+d8+" "+d9+" "+d10+" "+d11+" "+d12+" "+d13);
	  //System.out.println(""+c1+" "+c2+" "+c3+" "+c4+" "+c5+" "+c6+" "+c7+" "+c8+" "+c9+" "+c10+" "+c11+" "+c12+" "+c13);
	  a2=new pt(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13);
	  
	  ArrayList<Double> ag=new ArrayList<>();
		ag.add(c1);
		ag.add(c2);
		ag.add(c3);
		ag.add(c4);
		ag.add(c5);
		ag.add(c6);
		ag.add(c7);
		ag.add(c8);
		ag.add(c9);
		ag.add(c10);
		ag.add(c11);
		ag.add(c12);
		ag.add(c13);
		if(!map.containsKey(x2))
		map.put(x2,ag);
		else
		map.replace(x2, ag);
		if(!centroid.containsKey(x2))
		centroid.put(x2, ag);
		else
		centroid.replace(x2, ag);
	}
	else
	{
		int ko1=rand.nextInt(nodes.get(change).size());
		int ko2=rand.nextInt(nodes.get(change).size());
		ArrayList<Long>  a=nodes.get(change);
		long node1=a.get(ko1);
		long node2=a.get(ko2);
		 ArrayList<Double> ag=map.get(node1);
		 if(!map.containsKey(x1))
				map.put(x1,ag);
				else
				map.replace(x1, ag);
				if(!centroid.containsKey(x1))
				centroid.put(x1, ag);
				else
				centroid.replace(x1, ag);
				
		ArrayList<Double> ag1=map.get(node2);
		if(!map.containsKey(x2))
		map.put(x2,ag1);
		else
		map.replace(x2, ag1);
		if(!centroid.containsKey(x2))
		centroid.put(x2, ag1);
		else
		centroid.replace(x2, ag1);
	}
		if(check!=0)
			  john=nodes.get(change).size();
		
  for (int t = 0; t<4; ++t){
	// Create job
	  q=0;
	  
	Job job = Job.getInstance(conf, "Find K-Nearest Neighbour");
	job.setJarByClass(bkm.class);
	// Set the third parameter when running the job to be the parameter file and give it an alias
	//job.addCacheFile(new URI(args[2] + "#knnParamFile")); // Parameter file containing test data

	// Setup MapReduce job
	job.setMapperClass(kmeansMapper.class);
	job.setReducerClass(KmeansReducer.class);
	job.setNumReduceTasks(1); // Only one reducer in this design

	// Specify key / value
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(LongWritable.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);
	// Path op=new Path(args[1]);
	// Input (the data file) and Output (the resulting classification)
	FileInputFormat.addInputPath(job, new Path(args[0]));
	//if(i==0)
	
	FileSystem fs = FileSystem.get(conf);
	fs.delete(new Path(args[1]),true);
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	// Execute job and return status
	//System.exit(job.waitForCompletion(true) ? 0 : 1);
	job.waitForCompletion(true) ;
	job.close();
	//if(check==1&&y!=(K-1))
	fs.delete(new Path(args[1]),true);
	
    }
  	if(check==0)
  		check=1;
	}
System.out.println("||==================================||");	    
System.out.println("||Cluster Size||SSE                 ||");
System.out.println("||==================================||");
  for(int p=0;p<5;p++)
  {		Long m=Long.MIN_VALUE;
  		Long save=Long.MAX_VALUE;
	  ArrayList<Long> q=new ArrayList<>();
	  //String ans=new String();
  	for(long h=0;h<5;h++)
  	{
  		if(numbers.containsKey(h))
  		if(m<numbers.get(h))
  			{m=numbers.get(h);
  			save=h;}
  			
  	}
	  if(nodes.containsKey((long)p))
	  q=nodes.get((long)p);
	  //if(p!=save)
	  //DecimalFormat ft = new DecimalFormat("####"); 
	if(numbers.get((long)p)!=null)
	  System.out.printf("||%12d||%20.7f||\n",numbers.get((long)p),sse.get(p));
	ArrayList<Double> cat;
//	if(map.get(p)!=null)
//	{cat=map.get(p);
//	for(Double d:cat)
//	System.out.printf("%f",d);}
//	System.out.printf("\n");
  }
  System.out.println("||==================================||"); 	
  long endTime = System.currentTimeMillis();
  						  
      System.out.printf("||Time        ||%18d s||\n",(endTime - startTime)/1000 ); 
      System.out.println("||==================================||"); 
      
	}
	
}