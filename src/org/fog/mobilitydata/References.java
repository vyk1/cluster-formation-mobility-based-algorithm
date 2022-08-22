package org.fog.mobilitydata;

import java.io.File;

public class References {

	public static final int NOT_SET = -1;
	public static final double SETUP_TIME = -1.00;
	public static final double INIT_TIME = 0.00;

	public static final int DIRECTIONAL_MOBILITY = 1;
	public static final int RANDOM_MOBILITY = 2;

	// Reference geographical information to create random mobility pattern for
	// mobile users
	public static final double lat_reference = -37.81349283433532;
	public static final double long_reference = 144.952370512958;

	// Reference dataset filename to store and retrieve users positions
	public static final String dataset_reference = String.format(".%sdataset%slogical%susersLocation-melbCBD_",
			File.separator, File.separator, File.separator);
	public static final String output_path_data = String.format(".%sdataset%soutput_data.csv", File.separator,
			File.separator);
	public static final String dataset_random = String.format(".%sdataset%srandomic%srandom_usersLocation-melbCBD_",
			File.separator, File.separator, File.separator);
	public static final int random_walk_mobility_model = 1;
	public static final int random_waypoint_mobility_model = 2;
	public static double MinMobilitySpeed = 1; //
	public static double MaxMobilitySpeed = 2; //
	public static double environmentLimit = 6371; // shows the maximum latitude and longitude of the environment.
													// Currently it is set based on radius of the Earth (6371 KM)
}
