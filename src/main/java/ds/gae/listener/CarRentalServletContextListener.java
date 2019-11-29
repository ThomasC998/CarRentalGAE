package ds.gae.listener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;

import ds.gae.CarRentalModel;
import ds.gae.entities.Car;
import ds.gae.entities.CarRentalCompany;
import ds.gae.entities.CarType;

public class CarRentalServletContextListener implements ServletContextListener {
		
	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		// This will be invoked as part of a warming request,
		// or the first user request if no warming request was invoked.

		// check if dummy data is available, and add if necessary
		if (!isDummyDataAvailable()) {
			addDummyData();
		}
		
		// check that correct launch configuration is used
		if ("distributed-systems-gae".equals(System.getenv("DATASTORE_DATASET"))) {
			Logger.getLogger(CarRentalServletContextListener.class.getName())
			.log(Level.INFO, "Launch configuration correctly loaded");
		} else {
			Logger.getLogger(CarRentalServletContextListener.class.getName())
			.log(Level.SEVERE, "Launch configuration did not load! Restart using the correct launch configuration.");
			throw new RuntimeException("Launch configuration did not load!");
		}
	}

	private boolean isDummyDataAvailable() {
		// If the Hertz car rental company is in the datastore, we assume the dummy data
		// is available
		
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		// get all entities of type CRC
		Query<Entity> query = Query.newEntityQueryBuilder()
				.setKind("crc")
				.build();
		QueryResults<Entity> results = datastore.run(query);
		
//		return CarRentalModel.get().getAllRentalCompanyNames().contains("Hertz");
		return results.hasNext();
		
	}

	private void addDummyData() {
		loadRental("Hertz", "hertz.csv");
		loadRental("Dockx", "dockx.csv");
	}

	private void loadRental(String name, String datafile) {
		Logger.getLogger(CarRentalServletContextListener.class.getName()).log(Level.INFO, "loading {0} from file {1}",
				new Object[] { name, datafile });
		try {
			Set<Car> cars = loadData(name, datafile);
//			CarRentalCompany company = new CarRentalCompany(name, cars);
			// FIXME: use persistence instead
//            CarRentalModel.get().CRCS.put(name, company);

			// datastore
			
			
			// store crc in datastore
			Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
			Key crcKey = datastore.newKeyFactory()
					.setKind("crc")
					.newKey(name);
			Entity crcEntity = Entity.newBuilder(crcKey)
					.build();
			datastore.put(crcEntity);
			
			// store carTypes in datastore
			Set<CarType> cartypes = new HashSet<CarType>();
			for (Car car: cars) {
				cartypes.add(car.getType());
			}
			for (CarType cartype: cartypes) {
				String carTypeId = cartype.getName() + name;
				Key carTypeKey = datastore.newKeyFactory()
						.addAncestor(PathElement.of("crc", name))
						.setKind("cartype")
						.newKey(carTypeId);
				Entity carTypeEntity = Entity.newBuilder(carTypeKey)
						.set("name", cartype.getName())
						.set("nbOfSeats", cartype.getNbOfSeats())
						.set("smokingAllowed", cartype.isSmokingAllowed())
						.set("rentalPricePerDay", cartype.getRentalPricePerDay())
						.set("trunkSpace", cartype.getTrunkSpace())
						.build();
				datastore.put(carTypeEntity);
			}
			
			// store cars in datastore
			for (Car car: cars) {
				String carTypeId = car.getType().getName() + name;
//				System.out.println(carTypeId);
				Key carKey = datastore.newKeyFactory()
						.addAncestors(
//								PathElement.of("crc", name),
								PathElement.of("cartype", carTypeId))
						.setKind("car")
						.newKey(car.getId());
				Entity carEntity = Entity.newBuilder(carKey)
						.build();
				datastore.put(carEntity);
			}
			
			
			// datastore
			
		} catch (NumberFormatException ex) {
			Logger.getLogger(CarRentalServletContextListener.class.getName()).log(Level.SEVERE, "bad file", ex);
		} catch (IOException ex) {
			Logger.getLogger(CarRentalServletContextListener.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public static Set<Car> loadData(String name, String datafile) throws NumberFormatException, IOException {
		Set<Car> cars = new HashSet<Car>();
		int carId = 1;

		// open file from jar
		BufferedReader in = new BufferedReader(new InputStreamReader(
				CarRentalServletContextListener.class.getClassLoader().getResourceAsStream(datafile)));
		// while next line exists
		while (in.ready()) {
			// read line
			String line = in.readLine();
			// if comment: skip
			if (line.startsWith("#")) {
				continue;
			}
			// tokenize on ,
			StringTokenizer csvReader = new StringTokenizer(line, ",");
			// create new car type from first 5 fields
			CarType type = new CarType(csvReader.nextToken(), Integer.parseInt(csvReader.nextToken()),
					Float.parseFloat(csvReader.nextToken()), Double.parseDouble(csvReader.nextToken()),
					Boolean.parseBoolean(csvReader.nextToken()));
			// create N new cars with given type, where N is the 5th field
			for (int i = Integer.parseInt(csvReader.nextToken()); i > 0; i--) {
				cars.add(new Car(carId++, type));
			}
		}

		return cars;
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		// Please leave this method empty.
	}
}
