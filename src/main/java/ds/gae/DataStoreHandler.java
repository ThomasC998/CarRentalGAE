package ds.gae;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;

import ds.gae.entities.Car;
import ds.gae.entities.CarRentalCompany;
import ds.gae.entities.CarType;

// This class handles all interaction between the DataStore and the CarRentalModel
public class DataStoreHandler {
	
	private static Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
	
	private static KeyFactory crcKeyFactory = datastore.newKeyFactory().setKind("crc");
	private static KeyFactory carTypeKeyFactory = datastore.newKeyFactory().setKind("cartype");
	private static KeyFactory carKeyFactory = datastore.newKeyFactory().setKind("car");
	private static KeyFactory reservationKeyFactory = datastore.newKeyFactory().setKind("reservation");
	
	public DataStoreHandler() {
		
	}
	
	/************
	 * COMPANYS *
	 ************/
	
	public static void storeCRC(CarRentalCompany crc) {
		Key key = crcKeyFactory.newKey(crc.getName());
		
		Entity crcEntity = Entity.newBuilder(key)
				//.set("name", crc.getName())
			    .build();
		datastore.put(crcEntity);
	}
	
	public CarRentalCompany getCRC(String crcName) {
		Key crcKey = crcKeyFactory.newKey(crcName);
		Entity crcEntity = datastore.get(crcKey);
		
		// Get crc cars
		Set<Car> cars = getAllCompanyCars(crcName);

		CarRentalCompany crc = new CarRentalCompany(crcEntity.getKey().getName(), cars);
		return crc;
	}
	
	public static Collection<String> getAllRentalCompanyNames() {
		// get all entities of type CRC
		Query<Entity> query = Query.newEntityQueryBuilder()
				.setKind("crc")
				.build();
		QueryResults<Entity> results = datastore.run(query);
		
		// map entities in results to the name of the crc's
		Set<String> crcNames = new HashSet<String>();
		results.forEachRemaining( crcEntity -> {
			String crcName = crcEntity.getKey().getName();
			crcNames.add(crcName);
			System.out.println(crcName);
		});
		
		return crcNames;
	}
	
	/************
	 * CARTYPES *
	 ************/
	
	public static void storeCarType(CarType ct, String crcName) {
		String carTypeId = ct.getName() + crcName;
		Key key = carTypeKeyFactory
				.addAncestor(PathElement.of("crc", crcName))
				.setKind("cartype")
				.newKey(ct.getName());
		
		Entity ctEntity = Entity.newBuilder(key)
				.set("name", ct.getName())
				.set("nbOfSeats", ct.getNbOfSeats())
				.set("smokingAllowed",ct.isSmokingAllowed())
				.set("rentalPricePerDay", ct.getRentalPricePerDay())
				.set("trunkSpace", ct.getTrunkSpace())
				.build();
		datastore.put(ctEntity);
	}
	
	public Set<String> getCarTypesName(String companyName) {
		// get parent key
		Key parentCrcKey = crcKeyFactory.newKey(companyName);
		
		// get all cartypes that are children of the crc
		Query<Entity> query = Query.newEntityQueryBuilder()
				.setKind("cartype")
				.setFilter(PropertyFilter.hasAncestor(parentCrcKey))
				.build();
		QueryResults<Entity> results = datastore.run(query);
		
		// map entities in results to the name of the cartype
		Set<String> carTypeNames = new HashSet<String>();
		results.forEachRemaining( carTypeEntity -> {
			String carTypeName = carTypeEntity.getKey().getName();
			carTypeNames.add(carTypeName);
			System.out.println(carTypeName);
		});
		
		return carTypeNames;
		
	}
	
	/********
	 * CARS *
	 ********/
	
	public static void storeCar(Car car, String crcName) {
		String carTypeId = car.getType().getName() + crcName;
		Key key = carKeyFactory
				.addAncestor(PathElement.of("cartype", carTypeId))
				.setKind("car")
				.newKey(car.getId());
		
		Entity carEntity = Entity.newBuilder(key)
				.build();
		datastore.put(carEntity);
	}
	
	public Set<Car> getAllCompanyCars(String crcName) {
		Set<Car> cars = new HashSet<Car>();
		
		Set<String> carTypes = getCarTypesName(crcName);
		for (String ct : carTypes) {
			// get parent key for car
			Key parentCarTypeKey = carTypeKeyFactory.newKey(ct);
			
			// get all cars that are children of the cartype
			Query<Entity> query = Query.newEntityQueryBuilder()
					.setKind("car")
					.setFilter(PropertyFilter.hasAncestor(parentCarTypeKey))
					.build();
			QueryResults<Entity> results = datastore.run(query);
			
			// map entities in results to a car set
			Set<Car> carsSet = new HashSet<Car>();
			results.forEachRemaining( carEntity -> {
				Car carObj = new Car(Integer.parseInt(carEntity.getKey().getName()), ct);
				carsSet.add(carObj);
			});
			cars.addAll(carsSet);
		}
		return cars;
	}
	
	/****************
	 * RESERVATIONS *
	 ****************/
	
	
	
}
