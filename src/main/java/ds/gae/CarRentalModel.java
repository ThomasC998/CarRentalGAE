package ds.gae;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;

import ds.gae.entities.Car;
import ds.gae.entities.CarRentalCompany;
import ds.gae.entities.CarType;
import ds.gae.entities.Quote;
import ds.gae.entities.Reservation;
import ds.gae.entities.ReservationConstraints;

public class CarRentalModel {

	// FIXME use persistence instead
	//public Map<String,CarRentalCompany> CRCS = new HashMap<String, CarRentalCompany>();	

	private DataStoreHandler dataStoreHandler = new DataStoreHandler();
	
	private static CarRentalModel instance;

	public static CarRentalModel get() {
		if (instance == null) {
			instance = new CarRentalModel();
		}
		return instance;
	}

	/**
	 * Get the car types available in the given car rental company.
	 *
	 * @param companyName the car rental company
	 * @return The list of car types (i.e. name of car type), available in the given
	 *         car rental company.
	 */
	public Set<String> getCarTypesNames(String companyName) {
		Set<String> cartypeNames = new HashSet<String>();
		Set<CarType> cartypes = new HashSet<CarType>(dataStoreHandler.getCarTypesOfCarRentalCompany(companyName));
		for (CarType ct : cartypes) {
			cartypeNames.add(ct.getName());
		}
		return cartypeNames;
	}

	/**
	 * Get the names of all registered car rental companies
	 *
	 * @return the list of car rental companies
	 */
	public Collection<String> getAllRentalCompanyNames() {
		return dataStoreHandler.getAllRentalCompanyNames();
	}

	/**
	 * Create a quote according to the given reservation constraints (tentative
	 * reservation).
	 * 
	 * @param companyName name of the car renter company
	 * @param renterName  name of the car renter
	 * @param constraints reservation constraints for the quote
	 * @return The newly created quote.
	 * 
	 * @throws ReservationException No car available that fits the given
	 *                              constraints.
	 */
	public Quote createQuote(String companyName, String renterName, ReservationConstraints constraints)
			throws ReservationException {
		// FIXME: use persistence instead
//    	CarRentalCompany crc = CRCS.get(companyName);
		
		System.out.println("crModel.createQuote");
		
		CarRentalCompany crc = dataStoreHandler.getCRC(companyName);
		return crc.createQuote(constraints, renterName);
	}

	/**
	 * Confirm the given quote.
	 *
	 * @param quote Quote to confirm
	 * 
	 * @throws ReservationException Confirmation of given quote failed.
	 */
	public void confirmQuote(Quote quote) throws ReservationException {
		// FIXME: use persistence instead
//		CarRentalCompany crc = CRCS.get(quote.getRentalCompany());
		
		System.out.println("crModel.confirmQuote");
		
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		Key crcKey = datastore.newKeyFactory()
				.setKind("crc")
				.newKey(quote.getRentalCompany());
		
		Entity crcEntity = datastore.get(crcKey);
		
		Set<Car> cars = new HashSet<Car>();
		CarRentalCompany crc = new CarRentalCompany(crcEntity.getKey().getName(), cars);
		
		crc.confirmQuote(quote);
	}

	/**
	 * Confirm the given list of quotes
	 * 
	 * @param quotes the quotes to confirm
	 * @return The list of reservations, resulting from confirming all given quotes.
	 * 
	 * @throws ReservationException One of the quotes cannot be confirmed. Therefore
	 *                              none of the given quotes is confirmed.
	 */
	public List<Reservation> confirmQuotes(List<Quote> quotes) throws ReservationException {
		// TODO: add implementation when time left, required for GAE2
    	return null;
	}

	/**
	 * Get the list of cars of the given car type in the given car rental company.
	 *
	 * @param companyName name of the car rental company
	 * @param carType     the given car type
	 * @return A list of car IDs of cars with the given car type.
	 */
	public Collection<Integer> getCarIdsByCarType(String companyName, CarType carType) {
		Collection<Integer> out = new ArrayList<Integer>();
		for (Car c : getCarsByCarType(companyName, carType)) {
			out.add(c.getId());
		}
		return out;
	}

	/**
	 * Get the amount of cars of the given car type in the given car rental company.
	 *
	 * @param companyName name of the car rental company
	 * @param carType     the given car type
	 * @return A number, representing the amount of cars of the given car type.
	 */
	public int getAmountOfCarsByCarType(String companyName, CarType carType) {
		return this.getCarsByCarType(companyName, carType).size();
	}

	/**
	 * Get the list of cars of the given car type in the given car rental company.
	 *
	 * @param companyName name of the car rental company
	 * @param carType     the given car type
	 * @return List of cars of the given car type
	 */
	private List<Car> getCarsByCarType(String companyName, CarType carType) {
		
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		// get cartype key
		String carTypeId = carType.getName() + companyName;
		Key parentCarTypeKey = datastore.newKeyFactory()
				.addAncestor(PathElement.of("crc", companyName))
				.setKind("cartype")
				.newKey(carTypeId);
		
		List<Car> cars = new ArrayList<Car>();
		Query<Entity> query2 = Query.newEntityQueryBuilder()
				.setKind("car")
				.setFilter(PropertyFilter.hasAncestor(parentCarTypeKey))
				.build();
		QueryResults<Entity> carEntities = datastore.run(query2);
		
		carEntities.forEachRemaining( carEntity -> {
			int id = carEntity.getKey().getId().intValue();
			Car newCar = new Car(id,carType);
			cars.add(newCar);
		});
		
		
		return cars;
	}

	/**
	 * Check whether the given car renter has reservations.
	 *
	 * @param renter the car renter
	 * @return True if the number of reservations of the given car renter is higher
	 *         than 0. False otherwise.
	 */
	public boolean hasReservations(String renter) {
		return getReservations(renter).size() > 0;
	}
	
	/**
	 * Get all reservations made by the given car renter.
	 *
	 * @param renter name of the car renter
	 * @return the list of reservations of the given car renter
	 */
	//TODO: hasReservations cant find this function when put in the dataStoreHandler
	public List<Reservation> getReservations(String renter) {
    	
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		
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
}
