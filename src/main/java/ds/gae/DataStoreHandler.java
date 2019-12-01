package ds.gae;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
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
import ds.gae.entities.Quote;
import ds.gae.entities.Reservation;

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
				.newKey(carTypeId);
		
		Entity ctEntity = Entity.newBuilder(key)
				.set("name", ct.getName())
				.set("nbOfSeats", ct.getNbOfSeats())
				.set("smokingAllowed",ct.isSmokingAllowed())
				.set("rentalPricePerDay", ct.getRentalPricePerDay())
				.set("trunkSpace", ct.getTrunkSpace())
				.build();
		datastore.put(ctEntity);
	}
	
	/**
	 * Get the car types available in the given car rental company.
	 *
	 * @param companyName the given car rental company
	 * @return The list of car types in the given car rental company.
	 */
	public Collection<CarType> getCarTypesOfCarRentalCompany(String companyName) {
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		// get parent key
		Key parentCrcKey = datastore.newKeyFactory()
				.setKind("crc")
				.newKey(companyName);
		
		// get all cartypes that are children of the crc
		Query<Entity> query = Query.newEntityQueryBuilder()
				.setKind("cartype")
				.setFilter(PropertyFilter.hasAncestor(parentCrcKey))
				.build();
		QueryResults<Entity> results = datastore.run(query);
		
		// map entities in results to the name of the cartype
		List<CarType> cartypes = new ArrayList<CarType>();
		results.forEachRemaining( carTypeEntity -> {
			String carTypeName = carTypeEntity.getString("name");
			int nbOfSeats = ((Long) carTypeEntity.getLong("nbOfSeats")).intValue();
			boolean smokingAllowed = carTypeEntity.getBoolean("smokingAllowed");
			double rentalPricePerDay = carTypeEntity.getDouble("rentalPricePerDay");
			float trunkSpace = (float) carTypeEntity.getDouble("trunkSpace");
			CarType cartype = new CarType(carTypeName, nbOfSeats, trunkSpace, rentalPricePerDay, smokingAllowed);
			cartypes.add(cartype);
		});
		
		return cartypes;
	}
	
	/********
	 * CARS *
	 ********/
	
	public static void storeCar(Car car, String crcName) {
		String carTypeId = car.getType().getName() + crcName;
		Key key = carKeyFactory
				.addAncestors(
						PathElement.of("crc", crcName),
						PathElement.of("cartype", carTypeId))
				.newKey(car.getId());
		
		Entity carEntity = Entity.newBuilder(key)
				.build();
		datastore.put(carEntity);
	}
	
	public Set<Car> getAllCompanyCars(String crcName) {
		Set<Car> cars = new HashSet<Car>();
		
		Collection<CarType> carTypes = getCarTypesOfCarRentalCompany(crcName);
		for (CarType ct : carTypes) {
			// get parent key for car
			Key parentCarTypeKey = carTypeKeyFactory.newKey(ct.getName());
			
			// get all cars that are children of the cartype
			Query<Entity> query = Query.newEntityQueryBuilder()
					.setKind("car")
					.setFilter(PropertyFilter.hasAncestor(parentCarTypeKey))
					.build();
			QueryResults<Entity> results = datastore.run(query);
			
			// map entities in results to a car set
			Set<Car> carsSet = new HashSet<Car>();
			results.forEachRemaining( carEntity -> {
				int uid = Integer.parseInt(carEntity.getKey().getName());
				Set<Reservation> reservations = getCarReservations(uid);
				Car carObj = new Car(uid, ct, reservations);
				carsSet.add(carObj);
			});
			cars.addAll(carsSet);
		}
		return cars;
	}
	
	/****************
	 * RESERVATIONS *
	 ****************/
	
	/**
	 * Get all reservations made by the given car renter.
	 *
	 * @param renter name of the car renter
	 * @return the list of reservations of the given car renter
	 */
	public List<Reservation> getReservations(String renter) {
    	
		Query<Entity> query = Query.newEntityQueryBuilder()
				.setKind("reservation")
				.build();
		QueryResults<Entity> reservationEntities = datastore.run(query);

		List<Reservation> reservations = new ArrayList<Reservation>();	
		reservationEntities.forEachRemaining( reservationEntity -> {
			String renterName = reservationEntity.getString("renter");
			if (renterName.equals(renter)) {
				Date startDate = reservationEntity.getTimestamp("startDate").toDate();
				Date endDate = reservationEntity.getTimestamp("endDate").toDate();
				Double rentalPrice = reservationEntity.getDouble("rentalPrice");
				int carId = ((Long) reservationEntity.getLong("carId")).intValue();
				String carType = reservationEntity.getString("carType");
				String rentalCompany = reservationEntity.getString("rentalCompany");
				Quote quote = new Quote(renterName, startDate, endDate, rentalCompany, carType, rentalPrice);
				Reservation reservation = new Reservation(quote, carId);
				reservations.add(reservation);
			}
		});
    	
    	return reservations;
	}
	
	public Set<Reservation> getCarReservations(int uid) {
		Set<Reservation> reservations = new HashSet<Reservation>();	
		
		Query<Entity> query = Query.newEntityQueryBuilder()
				.setKind("reservation")
				.build();
		QueryResults<Entity> reservationEntities = datastore.run(query);

		reservationEntities.forEachRemaining( reservationEntity -> {
			int carId = ((Long) reservationEntity.getLong("carId")).intValue();
			if (carId == uid) {
				String renterName = reservationEntity.getString("renter");
				Date startDate = reservationEntity.getTimestamp("startDate").toDate();
				Date endDate = reservationEntity.getTimestamp("endDate").toDate();
				Double rentalPrice = reservationEntity.getDouble("rentalPrice");
				String carType = reservationEntity.getString("carType");
				String rentalCompany = reservationEntity.getString("rentalCompany");
				Quote quote = new Quote(renterName, startDate, endDate, rentalCompany, carType, rentalPrice);
				Reservation reservation = new Reservation(quote, carId);
				reservations.add(reservation);
			}
		});
		
		return reservations;
	}
	
	
	
}
