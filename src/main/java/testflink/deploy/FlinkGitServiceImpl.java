package testflink.deploy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.RemoteEnvironment;
import org.apache.flink.storm.api.FlinkClient;
import org.apache.flink.storm.api.FlinkSubmitter;
import org.apache.flink.storm.api.FlinkTopology;
//import org.apache.flink.storm.util.BoltFileSink;
//import org.apache.flink.storm.util.BoltPrintSink;
import org.apache.flink.storm.util.NullTerminatingSpout;
//import org.apache.flink.storm.util.OutputFormatter;
//import org.apache.flink.storm.util.TupleOutputFormatter;
//import org.apache.flink.storm.wordcount.operators.BoltCounter;
//import org.apache.flink.storm.wordcount.operators.BoltCounterByName;
//import org.apache.flink.storm.wordcount.operators.BoltTokenizer;
//import org.apache.flink.storm.wordcount.operators.BoltTokenizerByName;
//import org.apache.flink.storm.wordcount.operators.WordCountFileSpout;
//import org.apache.flink.storm.wordcount.operators.WordCountInMemorySpout;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.InvocationResult;
import org.apache.maven.shared.invoker.Invoker;
import org.apache.maven.shared.invoker.MavenInvocationException;
import org.codehaus.plexus.util.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.apache.flink.client.RemoteExecutor; 
import org.apache.flink.client.program.Client; 
import org.apache.flink.client.program.PackagedProgram; 
import org.apache.flink.client.program.ProgramInvocationException; 
import org.apache.flink.configuration.Configuration; 

import java.io.File; 
import java.net.InetSocketAddress; 

@Service
public class FlinkGitServiceImpl implements FlinkGitService{

	
	private static final Logger log = LoggerFactory.getLogger(FlinkGitService.class);
	private final static String USER_HOME_PATH = "/home";
	private String imageUser="ubuntu";

	private String imageUserPassword="";
	
	private String flinkPath="/home/streamly/flink";

	private String flinkPathForSsh="/home/ubuntu/flink";
	
	@Override
	public boolean build(String path, String ref, String url) {
		System.out.println("The path is :{} ref:{} and url:{}"+ path+" "+ ref+" "+url);
		String userName = path.split("/")[2];
		return callRef(userName, ref, path);

	}
	
	public boolean callRef(String username, String refs, String path) {

		String applicationName = path.split("/")[4];

		String target = null;
		FileInputStream fileInputStream = null;
		try {
			target = clone(username, applicationName, refs);
			InvocationRequest request = new DefaultInvocationRequest();
			request.setPomFile(new File(target + "/pom.xml"));
			request.setGoals(Arrays.asList("clean", "package"));
			Invoker invoker = new DefaultInvoker();
			invoker.setMavenHome(new File("/usr/share/maven"));
			InvocationResult result = null;
			try {
				result = invoker.execute(request);
				System.out.println("Result of mvn install :{}"+ result);
			} catch (MavenInvocationException e) {
				System.out.println("Error in callRef method : {}"+ e.getMessage());
				throw new IllegalArgumentException(e.getMessage());
				// return response;
			}

			String config = target + "/topology.properties";
			File configFile = new File(config);
			if (!configFile.exists()) {
				throw new IllegalStateException("No topology configuration file found in project");
			}
			fileInputStream = new FileInputStream(configFile);
			Properties properties = new Properties();
			properties.load(fileInputStream);
			deploy(target, applicationName, username, properties);
			return true;
		} catch (Exception e) {
			System.out.println("Unable to build project :{} associate to user :{} with message :{}"+ applicationName+" "+username+" "+
					e.getMessage());

			throw new IllegalArgumentException("Unable to build project " + applicationName + " associated to user "
					+ username + " caused by " + e.getMessage());
		} finally {
			if (target != null) {
				try {
					FileUtils.deleteDirectory(target);
				} catch (IOException e) {
					System.out.println("Cannot delete directory"+ e);
				}
			}
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e) {
					System.out.println("Cannot close input stream"+ e);
				}
			}
		}
	}
	
//	public boolean validateProperties(Properties properties) {
//		boolean isPropertiesValid = true;
//		try {
//
//			// String indexid = properties.getProperty("indexId");
//			String accessKey = properties.getProperty("accessKey");
//			String secretKey = properties.getProperty("secretKey");
//			String topologyMainClass = properties.getProperty("topologyMainClass");
//			String methodName = properties.getProperty("methodName");
//			String clusterId = properties.getProperty("clusterId");
//			if ((accessKey == null) || (secretKey == null) || (topologyMainClass == null) || (methodName == null)
//					|| (clusterId == null)) {
//				isPropertiesValid = false;
//			}
//		} catch (Exception e) {
//			throw new IllegalArgumentException(
//					"Missing topology's property  :  " + e.getCause() + " " + e.getMessage());
//		}
//		return isPropertiesValid;
//	}
	
	public File findJarFile(final File folder) {
		File largestFile = null;
		long largestFileSize = 0l;
		long temp = 0l;
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isFile()) {
				if (fileEntry.getName().endsWith(".jar")) {
					temp = fileEntry.length();
					if (temp > largestFileSize) {
						largestFileSize = temp;
						largestFile = fileEntry;
					}
				}
			}
		}
		return largestFile;
	}
	
	@SuppressWarnings("unchecked")
	public void deploy(String repository, String applicationName, String userName, Properties properties) {
		final File folder = new File(repository + "/target");
		File jarFile = null;

		//boolean isValid = validateProperties(properties);
		//if (isValid) {
			try {
				jarFile = findJarFile(folder);
				//File jarFile = new File("/home/raymond/Documents/programs/flink-1.0.1/examples/streaming");
				// FileInputStream input = new FileInputStream(jarFile);
				System.out.println("The file  {}  has the size of  {}  and the path of {}; "+ jarFile.getName()+" "+ jarFile.length()+" "+ 
						jarFile.getPath());


				String clusterId = properties.getProperty("clusterId");
				String accessKey = properties.getProperty("accessKey");
				String secretKey = properties.getProperty("secretKey");

				

				// Removing the .git applicationName
				applicationName = applicationName.split("\\.")[0];
				System.out.println("application name without .git is : {}"+ applicationName);


				
				properties.setProperty("logging-index-id",
						properties.getProperty("logging-index-id") + "-" + applicationName);
				properties.setProperty("metrics-index-id",
						properties.getProperty("metrics-index-id") + "-" + applicationName + "-metrics");

				String metricsIndex = properties.getProperty("metrics-index-id");
				String loggingIndex = properties.getProperty("logging-index-id") + "_" + metricsIndex;
				String indexId = metricsIndex + "*";

				int numberWorker = Integer.parseInt(properties.getProperty("workers"));
				int parallelism = Integer.parseInt(properties.getProperty("parallelism"));
				String topologyMainClass = properties.getProperty("topologyMainClass");
				
				
					
					// topology configuration
					String ipJobManager = "54.86.198.188";
					Config topologyConf = new Config();
					topologyConf.setDebug(true);
					topologyConf.setNumWorkers(numberWorker);
					topologyConf.setMaxTaskParallelism(parallelism);
					// extended configuration
					@SuppressWarnings("rawtypes")
					Map defaultConf = Utils.readStormConfig();
					@SuppressWarnings("rawtypes")
					Map conf = new HashMap();
						conf.put(Config.NIMBUS_HOST, ipJobManager);
//					conf.put("topology.workers", numberWorker);
//					conf.put("metrics.id", metricsIndex);
//					conf.put("topology.debug", true);
//					conf.put("topology.max.task.parallelism", parallelism);
					conf.put(Config.NIMBUS_THRIFT_PORT,  6123);
//					conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
//							defaultConf.get(Config.STORM_THRIFT_TRANSPORT_PLUGIN));
					// conf.putAll(topologyConf);
					String jarParth1 = jarFile.getAbsolutePath();
					System.out.println("Absolute path "+jarParth1);
					System.out.println(String.format("file://%s", jarParth1));
//					FlinkTopology topology = 
//							getFlinkTopology(String.format("file://%s", jarParth1),
//							properties.getProperty("topologyMainClass"), properties.getProperty("methodName"));

//					RemoteStreamEnvironment remote = new RemoteStreamEnvironment(ipJobManager, 6123, jarParth1);
//					remote.execute();
					//ExecutionEnvironemnt remote = ExecutionEnvironemnt.createRemoteEnvironment(ipJobManager, 6123, jarParth1);
					// upload the topology jar file to The Job manager
					 boolean wait = true; 
					/*
					try { 
			            PackagedProgram program = new PackagedProgram(jarFile, new String[2]); 
			            InetSocketAddress jobManagerAddress = 
			RemoteExecutor.getInetFromHostport(ipJobManager+":6123"); 

			            Client client = new Client(ipJobManager, new 
			Configuration(), program.getUserCodeClassLoader()); 

			            client.run(program, parallelism, wait); 

			        } catch (ProgramInvocationException e) { 
			            e.printStackTrace(); 
			        } 
					*/
					
//					String remoteJar = FlinkSubmitter.submitJar(conf, jarParth1);
//					
//					final FlinkClient cluster = FlinkClient.getConfiguredClient(conf);
					
//					// Save topology
//					TopologyGit newTopo = new TopologyGit();
//					newTopo.setId(UUID.randomUUID().toString());
//					newTopo.setAccountId(accountId);
//					newTopo.setDebug(true);
//					newTopo.setName(loggingIndex);
//					newTopo.setClusterName(clusterName);
//					newTopo.setWorkersTotal(numberWorker);
//					newTopo.setTasksTotal(parallelism);
//					newTopo.setClusterId(clusterId);
//					log.info("Config Map " + conf);
//					log.info("topologyMainClass " + properties.getProperty("topologyMainClass"));
//					log.info("methodName " + properties.getProperty("methodName"));
//					newTopo.setMainClass(properties.getProperty("topologyMainClass"));
//					newTopo.setMethodName(properties.getProperty("methodName"));
//					newTopo.setCreationDate(System.currentTimeMillis());
//					newTopo.setGitVersion(gitVersion());
//					newTopo.setStatus("ACTIVE");
//					log.info("topologyDAO : " + topologyDAO);
//					topologyDAO.createTopology(newTopo);
//					log.info("Logging Index is :{}", loggingIndex);
//					log.info("Metrics Index is :{}", metricsIndex);
//					log.info("Submit the jar on Nimbus");
					
//					try {
//						cluster.submitTopology(applicationName, jarParth1, topology);
						//client.submitTopology(applicationName, jarParth1, topology);
						//FlinkSubmitter.submitTopology("myTopo", conf, buildTopology());
						//.submitTopology(applicationName, remoteJar, JSONValue.toJSONString(conf),
								//topology);

						// FlinkSubmitter.submitTopology(args[0], conf,
						// FlinkTopology.createTopology(builder));
//					} catch (AlreadyAliveException e) {
//						e.printStackTrace();
//						System.out.println("Message of exception generated :{}"+ e.getMessage());
//						throw new IllegalArgumentException(e.get_msg());
//					}
//					
					log.info("Submitting topology to flink");
					//submitTopologyToFlink(masterIp, jarPath);
					copyJarToJobManager(ipJobManager, jarParth1);
					submitFlinkTopologyBySsh(ipJobManager, topologyMainClass, jarFile.getName());
					log.info("Topology submitted successfully");
				

				System.out.println("Topology deployed successfully");

			} catch (Exception e) {
				System.out.println(" Exception " + e.getMessage() + " caused by " + e.getCause());
				e.printStackTrace();
				throw new IllegalArgumentException(e.getMessage());
			}
	}
	/*
	public static FlinkTopology buildTopology1() {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new RandomSentenceSpout(), 1);
		builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
		builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));

		builder.setBolt("writeIntoFile", new BoltFileSink("/home/raymond/wordcount.txt", new OutputFormatter() {
			private static final long serialVersionUID = 1L;

			@Override
			public String format(Tuple tuple) {
				return tuple.toString();
			}
		}), 1).shuffleGrouping("count");

		Config conf = new Config();

		conf.put("metrics.id", "metricsindex");
		return FlinkTopology.createTopology(builder);

	}
	*/
	
	public final static String spoutId = "source";
	public final static String tokenierzerId = "tokenizer";
	public final static String counterId = "counter";
	public final static String sinkId = "sink";
	//private final static OutputFormatter formatter = new TupleOutputFormatter();
/*
	public static FlinkTopology buildTopology() {
		return buildTopology(true);
	}
	*/

	/*
	public static FlinkTopology buildTopology(boolean indexOrName) {

		final TopologyBuilder builder = new TopologyBuilder();

		// get input data
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = textPath.split(":");
			final String inputFile = tokens[tokens.length - 1];
			// inserting NullTerminatingSpout only required to stabilize integration test
			builder.setSpout(spoutId, new NullTerminatingSpout(new WordCountFileSpout(inputFile)));
		} else {
			builder.setSpout(spoutId, new WordCountInMemorySpout());
		}

		if (indexOrName) {
			// split up the lines in pairs (2-tuples) containing: (word,1)
			builder.setBolt(tokenierzerId, new BoltTokenizer(), 4).shuffleGrouping(spoutId);
			// group by the tuple field "0" and sum up tuple field "1"
			builder.setBolt(counterId, new BoltCounter(), 4).fieldsGrouping(tokenierzerId,
					new Fields(BoltTokenizer.ATTRIBUTE_WORD));
		} else {
			// split up the lines in pairs (2-tuples) containing: (word,1)
			builder.setBolt(tokenierzerId, new BoltTokenizerByName(), 4).shuffleGrouping(
					spoutId);
			// group by the tuple field "0" and sum up tuple field "1"
			builder.setBolt(counterId, new BoltCounterByName(), 4).fieldsGrouping(
					tokenierzerId, new Fields(BoltTokenizerByName.ATTRIBUTE_WORD));
		}

		// emit result
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = outputPath.split(":");
			final String outputFile = tokens[tokens.length - 1];
			builder.setBolt(sinkId, new BoltFileSink(outputFile, formatter)).shuffleGrouping(counterId);
		} else {
			builder.setBolt(sinkId, new BoltPrintSink(formatter), 4).shuffleGrouping(counterId);
		}

		return FlinkTopology.createTopology(builder);
	}
*/
	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileInputOutput = false;
	private static String textPath;
	private static String outputPath;

	static boolean parseParameters(final String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileInputOutput = true;
			if (args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Usage: WordCount* <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing WordCount example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println("  Usage: WordCount* <text path> <result path>");
		}

		return true;
	}
	
	private FlinkTopology getFlinkTopology(String path, String className, String methodName)
			throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		ClassLoader loader = URLClassLoader.newInstance(new URL[] { new URL(path) }, FlinkGitService.class.getClassLoader());
		Class<?> clazz = loader.loadClass(className);
		Method method = clazz.getMethod(methodName, new Class[] {});
		System.out.println("method.getParameterCount = "+method.getParameterCount());
		System.out.println("method.getName = "+method.getName());
		System.out.println("method.toString = "+method.toString());
		return (FlinkTopology) method.invoke(null, new Object[] {});
	}

	private boolean copyJarToJobManager(String jobManagerPublicIp, String flinkTopologyPath) {
		try {
			String cmd = "scp  " + flinkTopologyPath + " "+"ubuntu@" + jobManagerPublicIp+":/home/ubuntu";
			log.info("Command used to copy jar :{}",cmd);
			Process p = Runtime.getRuntime().exec(cmd);

			p.waitFor();
			StringBuffer output = new StringBuffer();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
			log.info("Flink topology copied successfully :{} ", output);
			return true;
		} catch (Exception e) {
			log.error("Problem while copying topology " + e.getMessage());
			throw new IllegalArgumentException("Problem while copying topology " + e.getMessage());
		}

	}

	private boolean submitFlinkTopologyBySsh(String jobManagerPublicIp,String topologyMainClass, String jarName) {

		log.info("ssh the job manager of the cluster");
		String host = jobManagerPublicIp;
		String user = imageUser;
		String password = imageUserPassword;
		String command = flinkPath + "/bin/flink run -c " +topologyMainClass+" /home/ubuntu/"+ jarName;
		log.info("Command used to submit toplogy :{}",command);
		

		JSch jsch = new JSch();
		jsch.setConfig("StrictHostKeyChecking", "no");
		com.jcraft.jsch.Session session;
		try {
			session = jsch.getSession(user, host, 22);
		} catch (JSchException e) {
			log.error("Problem while accessing our instance");
			throw new IllegalStateException("Problem while accessing our instance");
		}
		session.setPassword(password);
		try {
			session.connect();
		} catch (JSchException e) {
			log.error("Problem while connecting to our instance");
			throw new IllegalStateException("Problem while connecting to our instance");
		}
		log.info("Successfully connected to : {}", host);

		Channel channel2;
		try {
			channel2 = session.openChannel("exec");
			((ChannelExec) channel2).setCommand(command);
			channel2.setInputStream(null);
			((ChannelExec) channel2).setErrStream(System.err);
			InputStream in = channel2.getInputStream();
			channel2.connect();
			byte[] tmp = new byte[1024];
			while (true) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
					log.info(new String(tmp, 0, i));
				}
				if (channel2.isClosed()) {
					if (channel2.getExitStatus() == 0) {
						log.info("Everything went on well exit-status:{}", channel2.getExitStatus());
					} else {
						log.error("Problem while submitting the topology exit-status:{}", channel2.getExitStatus());
						throw new IllegalStateException(
								"Problem while submitting the topology exit-status " + channel2.getExitStatus());
					}
					log.info("Topology submit successfully to the job manager :{}", jobManagerPublicIp);
					break;
				}
			}
			channel2.disconnect();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException("Problem wihrmessage " + e.getMessage() + " cause " + e.getCause());
		}

	}
	
	public String clone(String username, String applicationName, String refs)
			throws IOException, InterruptedException, InvalidRemoteException, TransportException, GitAPIException {
		// For development mode
		 String applicationPath = USER_HOME_PATH + "/" + username +
		 "/repositories/" + applicationName ;
		// // For production mode
//		String applicationPath = username + "@" + serverIp + ":" + USER_HOME_PATH + "/" + username + "/repositories/"
//				+ applicationName;

		System.out.println("applicationPath :{}"+ applicationPath);
		String target = "/tmp/" + UUID.randomUUID() + "/";
		Git.cloneRepository().setURI(applicationPath).setDirectory(new File(target)).call();
		return target;
	}

	
}
