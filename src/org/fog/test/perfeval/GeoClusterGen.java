package org.fog.test.perfeval;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.BwProvisionerOverbooking;
import org.cloudbus.cloudsim.sdn.overbooking.PeProvisionerOverbooking;
import org.fog.application.AppEdge;
import org.fog.application.AppLoop;
import org.fog.application.Application;
import org.fog.application.selectivity.FractionalSelectivity;
import org.fog.entities.Actuator;
import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.entities.MicroserviceFogDevice;
import org.fog.entities.Sensor;
import org.fog.entities.Tuple;
import org.fog.mobilitydata.DataParser;
import org.fog.mobilitydata.Location;
import org.fog.mobilitydata.RandomMobilityGenerator;
import org.fog.mobilitydata.References;
import org.fog.placement.LocationHandler;
import org.fog.policy.AppModuleAllocationPolicy;
import org.fog.scheduler.StreamOperatorScheduler;
import org.fog.utils.Config;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;
import org.fog.utils.distribution.DeterministicDistribution;
import org.json.simple.parser.ParseException;

/**
 * Setup for Cluster Generation
 * 1. creation of clusters among fog nodes using dynamic clustering
 * 2. mobility of end user devices and microservice migration
 *
 * @author Victoria Botelho Martins (https://github.com/vyk1/)
 */

public class GeoClusterGen {
	static List<FogDevice> fogDevices = new ArrayList<FogDevice>();
	static List<Sensor> sensors = new ArrayList<Sensor>();
	static List<Actuator> actuators = new ArrayList<Actuator>();

	static Map<Integer, Integer> userMobilityPattern = new HashMap<Integer, Integer>();
	static LocationHandler locator;

	static double SENSOR_TRANSMISSION_TIME = 10;
	static int numberOfMobileUser = 5;

	// if random mobility generator for users is True, new random dataset will be
	// created for each user
	static boolean randomMobility_generator = false; // To use random datasets
	static boolean renewDataset = false; // To overwrite existing random datasets
	static List<Integer> clusteringLevels = new ArrayList<Integer>(); // The selected fog layers for clustering

	// application
	static List<Application> applications = new ArrayList<>();
	static List<HashSet<String>> clusters = null;

	public static void main(String[] args) {

		try {

			Log.disable();
			int num_user = 1; 
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; 

			CloudSim.init(num_user, calendar, trace_flag);

			FogBroker broker = new FogBroker("broker");

			Application microservicesApplication = createApplication("example", broker.getId());
			applications.add(microservicesApplication);

			//
			DataParser dataObject = new DataParser();
			locator = new LocationHandler(dataObject);

			String datasetReference = References.dataset_reference;

			if (randomMobility_generator) {
				datasetReference = References.dataset_random;
				createRandomMobilityDatasets(References.random_walk_mobility_model, datasetReference, renewDataset);
			}

			/**
			 * Clustered Fog node creation. 01. Create devices (Client,FON,FCN,Cloud) 02.
			 * Generate cluster connection. 03. Identify devices monitored by each FON
			 */
			createMobileUser(broker.getId(), applications.get(0).getAppId(), datasetReference);
			createFogDevices(broker.getId(), applications.get(0).getAppId());
			Log.printLine("Clusters generated. Check README.md for further details.");

		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}

	private static void createRandomMobilityDatasets(int mobilityModel, String datasetReference, boolean renewDataset)
			throws IOException, ParseException {
		RandomMobilityGenerator randMobilityGenerator = new RandomMobilityGenerator();
		for (int i = 0; i < numberOfMobileUser; i++) {

			randMobilityGenerator.createRandomData(mobilityModel, i + 1, datasetReference, renewDataset);
		}
	}

	/**
	 * Creates the fog devices in the physical topology of the simulation.
	 *
	 * @param userId
	 */
	private static void createFogDevices(int userId, String appId) throws NumberFormatException, IOException {
		locator.parseResourceInfo();
		List<String> proxies = new ArrayList<String>();
		List<String> gateways = new ArrayList<String>();
		List<String> added = new ArrayList<String>();

		/*
		 * Gathers all nodes
		 */
		List<String> nodes = locator.getLevelWiseResources(locator.getLevelID("Proxy"));
		nodes.addAll(locator.getLevelWiseResources(locator.getLevelID("Gateway")));

		double metersPerNode = Config.AREA / nodes.size();
		double suggestedRange = Math.ceil(Config.AREA / metersPerNode);
		double maxC = Math.ceil(nodes.size() / metersPerNode);
		System.out.println("Area/node: " + metersPerNode);
		System.out.println("Suggested range: " + suggestedRange);
		System.out.println("Total nodes: " + nodes.size());
		System.out.println("Total nodes/Range per node: " + maxC);
		System.out.printf("-----\n");

//		double switchVar = metersPerNode;
//		metersPerNode = suggestedRange;
//		suggestedRange = switchVar;
//		
//		maxC = Math.ceil(nodes.size() / metersPerNode);

		/*
		 * Foreach of them
		 */
		for (int i = 0; i < nodes.size(); i++) {
			String current = nodes.get(i);
			Integer responsible = null;
			/*
			 * Initializes the clusters for the first case
			 */
			if (clusters == null) {
				clusters = new ArrayList<HashSet<String>>();
				HashSet<String> newList = new HashSet<String>();
				List<String> z = new ArrayList<String>();
				z.add(current);
				newList.addAll(z);
				clusters.add(newList);
				responsible = 0;
				added.add(current);
			} else {
				/*
				 * If responsible is empty
				 */
				if (responsible == null) {
					/*
					   Iterates each cluster 
					 */
					for (int j = 0; j < clusters.size(); j++) {
						HashSet<String> list = clusters.get(j);
						/*
						   As they are in a group, checks if there is any cluster with the current node id 
						   and returns the responsible of the cluster (proxy)
						 */
						for (String node : list) {
							if (node == current) {
								responsible = j;
								break;
							}
						}
					}
				}
				/*
				   If still, there is no responsible, the current node is a proxy (responsible) node
				   and it is added to the clusters array
				 */
				if (responsible == null) {
					HashSet<String> newList = new HashSet<String>();
					List<String> z = new ArrayList<String>();
					z.add(current);
					newList.addAll(z);
					clusters.add(newList);
					responsible = clusters.size() - 1;
					added.add(current);
				}
			}

			/*
			  For each of the next nodes, we check if the blocks is not full (length < available - MAX)
			  and if the node is within the Euclidian Distance (range - ANR)
			 */
			for (int j = 0; j < nodes.size(); j++) {
				String nextNode = nodes.get(j);
				if (nextNode == current) {
					continue;
				}

				if (added.contains(nextNode)) {
					continue;
				}

				if (clusters.get(responsible).size() <= maxC) {
					if (calculateInRange(locator.getCoordinates(current), locator.getCoordinates(nextNode),
							suggestedRange)) {
						clusters.get(responsible).add(nextNode);
						added.add(nextNode);
//						System.out.printf("Total node %s\n", nodes.size());
//						System.out.println("Removed index " + j + " with value " + nextNode);
					}
				} 
			}
//			System.out.printf("The responsible node of %s is %s that has %s nodes\n", current, responsible,
//					clusters.get(responsible));
		}
//		System.out.println("Total clusters: " + clusters.size());
		int count = 0;
		Set<String> set = new HashSet<String>();
		for (int c = 0; c < clusters.size(); c++) {
			count += clusters.get(c).size();
			set.addAll(clusters.get(c));
		}
		System.out.println("Number of assigned nodes: " + set.size());
		System.out.println("Times nodes were assigned:" + count);

		/*
		  Gets only the clusters that has more than one node 
		  since for each cluster, it is needed at least one proxy and one gateway (2 nodes)
		 */

		ArrayList<HashSet<String>> selected = new ArrayList<HashSet<String>>();
		for (int k = 0; k < clusters.size(); k++) {
			if (clusters.get(k).size() > 1) {
				selected.add(clusters.get(k));
				Optional<String> firstOptional = clusters.get(k).stream().findFirst();
				String first = firstOptional.get();
				proxies.add(first);
				clusters.get(k).remove(first);
			}
		}
		System.out.println("Selected: " + selected.size());
		System.out.println("Proxies: " + proxies.size());

		FogDevice cloud = createFogDevice("cloud", 44800, 40000, 100, 10000, 0.01, 16 * 103, 16 * 83.25,
				MicroserviceFogDevice.CLOUD); // creates the fog device Cloud at the apex of the hierarchy with
												// level=0
		cloud.setParentId(References.NOT_SET);
		locator.linkDataWithInstance(cloud.getId(), locator.getLevelWiseResources(locator.getLevelID("Cloud")).get(0));
		cloud.setLevel(0);
		fogDevices.add(cloud);
		Location mapaC = locator.getCoordinates(locator.getLevelWiseResources(locator.getLevelID("Cloud")).get(0));
		CSV.write(String.valueOf(mapaC.latitude), String.valueOf(mapaC.longitude), "0", "0", "-1", "Datacenter");
		

		int count1 = 0;
		for (int k = 0; k < selected.size(); k++) {
			String proxyId = proxies.get(k);
			FogDevice proxy = createFogDevice("proxy-server_" + k, 2800, 4000, 10000, 10000, 0.0, 107.339, 83.4333,
					MicroserviceFogDevice.FON); // creates the fog device Proxy Server (level=1)
			Location mapa = locator.getCoordinates(proxyId);
			CSV.write(String.valueOf(mapa.latitude), String.valueOf(mapa.longitude), String.valueOf(k + 1), "1", "0",
					"Block " + (k + 1) + " Proxy");
			locator.linkDataWithInstance(proxy.getId(), proxyId);
			proxy.setParentId(cloud.getId()); // setting Cloud as parent of the Proxy Server
			proxy.setUplinkLatency(100); // latency of connection from Proxy Server to the Cloud is 100 ms
			proxy.setLevel(1);
			fogDevices.add(proxy);

			for (int j = 0; j < selected.get(k).size(); j++) {
				count1++;
				Object[] aux = selected.get(k).toArray();
				String gatewayId = (String) aux[j];
				gatewayId = gatewayId.toString();
				FogDevice gateway = createFogDevice("gateway_" + count1, 2800, 4000, 10000, 10000, 0.0, 107.339,
						83.4333, MicroserviceFogDevice.FCN);
				locator.linkDataWithInstance(gateway.getId(), gatewayId);
				Location mapaP = locator.getCoordinates(gatewayId);
				CSV.write(String.valueOf(mapaP.latitude), String.valueOf(mapaP.longitude), String.valueOf(k + 1), "2",
						String.valueOf(k + 1), "GW " + count1);
				gateway.setParentId(proxy.getId());
				gateway.setUplinkLatency(4);
				gateway.setLevel(2);
				fogDevices.add(gateway);
			}
		}

		Date date = new Date();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);

		String customTimestamp = String.valueOf(cal.get(Calendar.MONTH)) + "_"
				+ String.valueOf(cal.get(Calendar.DAY_OF_MONTH) + 1) + "_" + String.valueOf(cal.get(Calendar.YEAR))
				+ "_" + String.valueOf(cal.getTimeInMillis());
		try (PrintWriter writer = new PrintWriter(new File(
				String.format(".%sdataset%sedgeResources-%s.csv", File.separator, File.separator, customTimestamp)))) {
			StringBuilder csv = CSV.getCsv();
			writer.print(csv.toString());
			csv.setLength(0);
			writer.close();
			System.out.println("-----");
			System.out.println("Written as edgeResources-" + customTimestamp + ".csv");

		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}

	}

	private static boolean calculateInRange(Location loc1, Location loc2, double fogRange) {

		final int R = 6371; // Radius of the earth in Kilometers

		double latDistance = Math.toRadians(loc1.latitude - loc2.latitude);
		double lonDistance = Math.toRadians(loc1.longitude - loc2.longitude);
		double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(loc1.latitude))
				* Math.cos(Math.toRadians(loc2.latitude)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double distance = R * c; // kms

		distance = Math.pow(distance, 2);

		if (Math.sqrt(distance) <= fogRange / 1000) {
			return true;
		} else {
			return false;
		}

	}

	private static void createMobileUser(int userId, String appId, String datasetReference) throws IOException {

		for (int id = 1; id <= numberOfMobileUser; id++)
			userMobilityPattern.put(id, References.DIRECTIONAL_MOBILITY);

		locator.parseUserInfo(userMobilityPattern, datasetReference);

		List<String> mobileUserDataIds = locator.getMobileUserDataId();

		for (int i = 0; i < numberOfMobileUser; i++) {
			FogDevice mobile = addMobile("mobile_" + i, userId, appId, References.NOT_SET); // adding mobiles to the
																							// physical topology.
																							// Smartphones have been
																							// modeled as fog devices as
																							// well.
			mobile.setUplinkLatency(2); // latency of connection between the smartphone and proxy server is 2 ms
			locator.linkDataWithInstance(mobile.getId(), mobileUserDataIds.get(i));
			mobile.setLevel(3);

			fogDevices.add(mobile);
		}

	}

	private static MicroserviceFogDevice createFogDevice(String nodeName, long mips, int ram, long upBw, long downBw,
			double ratePerMips, double busyPower, double idlePower, String deviceType) {

		List<Pe> peList = new ArrayList<Pe>();

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerOverbooking(mips))); // need to store Pe id and MIPS Rating

		int hostId = FogUtils.generateEntityId();
		long storage = 1000000; // host storage
		int bw = 10000;

		PowerHost host = new PowerHost(hostId, new RamProvisionerSimple(ram), new BwProvisionerOverbooking(bw), storage,
				peList, new StreamOperatorScheduler(peList), new FogLinearPowerModel(busyPower, idlePower));

		List<Host> hostList = new ArrayList<Host>();
		hostList.add(host);

		String arch = "x86"; // system architecture
		String os = "Linux"; // operating system
		String vmm = "Xen";
		double time_zone = 10.0; // time zone this resource located
		double cost = 3.0; // the cost of using processing in this resource
		double costPerMem = 0.05; // the cost of using memory in this resource
		double costPerStorage = 0.001; // the cost of using storage in this
		// resource
		double costPerBw = 0.0; // the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN
		// devices by now

		FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(arch, os, vmm, host, time_zone, cost,
				costPerMem, costPerStorage, costPerBw);

		MicroserviceFogDevice fogdevice = null;
		try {
			fogdevice = new MicroserviceFogDevice(nodeName, characteristics, new AppModuleAllocationPolicy(hostList),
					storageList, 10, upBw, downBw, 1250000, 0, ratePerMips, deviceType);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return fogdevice;
	}

	private static FogDevice addMobile(String name, int userId, String appId, int parentId) {
		FogDevice mobile = createFogDevice(name, 500, 20, 1000, 270, 0, 87.53, 82.44, MicroserviceFogDevice.CLIENT);
		mobile.setParentId(parentId);
		// locator.setInitialLocation(name,drone.getId());
		Sensor mobileSensor = new Sensor("sensor-" + name, "M-SENSOR", userId, appId,
				new DeterministicDistribution(SENSOR_TRANSMISSION_TIME)); // inter-transmission time of EEG sensor
																			// follows a deterministic distribution
		mobileSensor.setApp(applications.get(0));
		sensors.add(mobileSensor);
		Actuator mobileDisplay = new Actuator("actuator-" + name, userId, appId, "M-DISPLAY");
		actuators.add(mobileDisplay);

		mobileSensor.setGatewayDeviceId(mobile.getId());
		mobileSensor.setLatency(6.0); // latency of connection between EEG sensors and the parent Smartphone is 6 ms

		mobileDisplay.setGatewayDeviceId(mobile.getId());
		mobileDisplay.setLatency(1.0); // latency of connection between Display actuator and the parent Smartphone is 1
										// ms
		mobileDisplay.setApp(applications.get(0));

		return mobile;
	}

	private static Application createApplication(String appId, int userId) {
		Application application = Application.createApplication(appId, userId); 

		
		application.addAppModule("clientModule", 10);
		application.addAppModule("processingModule", 10); 
		application.addAppModule("storageModule", 10); 

		if (SENSOR_TRANSMISSION_TIME == 5.1)
			application.addAppEdge("M-SENSOR", "clientModule", 2000, 500, "M-SENSOR", Tuple.UP, AppEdge.SENSOR);
		else
			application.addAppEdge("M-SENSOR", "clientModule", 3000, 500, "M-SENSOR", Tuple.UP, AppEdge.SENSOR);

		application.addAppEdge("clientModule", "processingModule", 3500, 500, "RAW_DATA", Tuple.UP, AppEdge.MODULE);

		application.addAppEdge("processingModule", "storageModule", 1000, 1000, "PROCESSED_DATA", Tuple.UP,
				AppEdge.MODULE);

		application.addAppEdge("processingModule", "clientModule", 14, 500, "ACTION_COMMAND", Tuple.DOWN,
				AppEdge.MODULE); 

		application.addAppEdge("clientModule", "M-DISPLAY", 1000, 500, "ACTUATION_SIGNAL", Tuple.DOWN,
				AppEdge.ACTUATOR); 

		/*
		 * Defining the input-output relationships (represented by selectivity) of the
		 * application modules.
		 */
		application.addTupleMapping("clientModule", "M-SENSOR", "RAW_DATA", new FractionalSelectivity(1.0)); 
		application.addTupleMapping("processingModule", "RAW_DATA", "PROCESSED_DATA", new FractionalSelectivity(1.0));
		application.addTupleMapping("processingModule", "RAW_DATA", "ACTION_COMMAND", new FractionalSelectivity(1.0)); 
		application.addTupleMapping("clientModule", "ACTION_COMMAND", "ACTUATION_SIGNAL",
				new FractionalSelectivity(1.0));
		application.setSpecialPlacementInfo("storageModule", "cloud");
		/*
		 * Defining application loops to monitor the latency of. Here, we add only one
		 * loop for monitoring : EEG(sensor) -> Client -> Concentration Calculator ->
		 * Client -> DISPLAY (actuator)
		 */
		final AppLoop loop1 = new AppLoop(new ArrayList<String>() {
			{
				add("M-SENSOR");
				add("clientModule");
				add("processingModule");
				add("clientModule");
				add("M-DISPLAY");
			}
		});
		List<AppLoop> loops = new ArrayList<AppLoop>() {
			{
				add(loop1);
			}
		};
		application.setLoops(loops);

		return application;
	}

}