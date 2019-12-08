package ds.gae;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.cloud.datastore.Transaction;

import ds.gae.entities.Car;
import ds.gae.entities.CarRentalCompany;
import ds.gae.entities.CarType;
import ds.gae.entities.Quote;
import ds.gae.entities.Reservation;
import ds.gae.entities.ReservationConstraints;

public class CarRentalModel {

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
		Set<String> carTypeNames = new HashSet<String>();
		results.forEachRemaining( carTypeEntity -> {
			String carTypeName = carTypeEntity.getKey().getName();
			carTypeNames.add(carTypeName);
		});
		
		return carTypeNames;
	}

	/**
	 * Get the names of all registered car rental companies
	 *
	 * @return the list of car rental companies
	 */
	public Collection<String> getAllRentalCompanyNames() {

		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
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
		});
		
		return crcNames;
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
		
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		Key crcKey = datastore.newKeyFactory()
				.setKind("crc")
				.newKey(companyName);
		
		Entity crcEntity = datastore.get(crcKey);
		
		Set<Car> cars = new HashSet<Car>();
		CarRentalCompany crc = new CarRentalCompany(crcEntity.getKey().getName(), cars);
		
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
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		Transaction tx = datastore.newTransaction();
		
		List<Reservation> reservations = new ArrayList<Reservation>();
		try {
			for (Quote quote: quotes) {
				Reservation reservation = confirmQuoteInTransaction(quote, tx);
				for (Reservation alreadyConfirmedReservation: reservations) {
					if (alreadyConfirmedReservation.getCarId() == reservation.getCarId()
							&& alreadyConfirmedReservation.getRentalCompany().equals(reservation.getRentalCompany())) {
						throw new ReservationException("Reservation failed, this renter tried to reserve the car with id "
							+ reservation.getCarId() + " of rental company " + reservation.getRentalCompany() + " more than once.");
					}
				}
				reservations.add(reservation);
			}
			tx.commit();
		} finally {
			System.out.println("finally");
			if (tx.isActive()) {
				System.out.println("rollback");
				tx.rollback();
			}
		}
    	return reservations;
	}
	
	private Reservation confirmQuoteInTransaction(Quote quote, Transaction tx) throws ReservationException {
		
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		Key crcKey = datastore.newKeyFactory()
				.setKind("crc")
				.newKey(quote.getRentalCompany());
		
		Entity crcEntity = datastore.get(crcKey);
		
		Set<Car> cars = new HashSet<Car>();
		CarRentalCompany crc = new CarRentalCompany(crcEntity.getKey().getName(), cars);
		
		System.out.println("getAvailableCar in confirmQuoteInTransaction");
		Car car = crc.getAvailableCar(quote);
		Reservation res = new Reservation(quote, car.getId());

		String carTypeId = quote.getCarType();
		KeyFactory keyFactory = datastore.newKeyFactory()
				.addAncestors(
						PathElement.of("crc", quote.getRentalCompany()),
						PathElement.of("cartype", carTypeId),
						PathElement.of("car", car.getId()))
				.setKind("reservation");
		Key reservationKey = datastore.allocateId(keyFactory.newKey());
		Entity reservationEntity = Entity.newBuilder(reservationKey)
				.set("renter", quote.getRenter())
				.set("startDate", Timestamp.of(quote.getStartDate()))
				.set("endDate", Timestamp.of(quote.getEndDate()))
				.set("rentalPrice", quote.getRentalPrice())
				.build();
		
		tx.put(reservationEntity);
		System.out.println("reservations");
		System.out.println(getReservations(quote.getRenter()));
		
		return res;
	}

	/**
	 * Get all reservations made by the given car renter.
	 *
	 * @param renter name of the car renter
	 * @return the list of reservations of the given car renter
	 */
	public List<Reservation> getReservations(String renter) {

		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		Query<Entity> query = Query.newEntityQueryBuilder()
				.setKind("reservation")
				.setFilter(PropertyFilter.eq("renter", renter))
				.build();
		QueryResults<Entity> reservationEntities = datastore.run(query);

		List<Reservation> reservations = new ArrayList<Reservation>();	
		reservationEntities.forEachRemaining( reservationEntity -> {
			List<PathElement> reservationAncestors = reservationEntity.getKey().getAncestors();
			String crcName = "";
			String carTypeName = "";
			int carId = 0;
			for (PathElement reservationAncestor: reservationAncestors) {
				if(reservationAncestor.getKind().equals("crc")) {
					crcName = reservationAncestor.getName();
				} else if(reservationAncestor.getKind().equals("cartype")) {
					carTypeName = reservationAncestor.getName();
				} else if(reservationAncestor.getKind().equals("car")) {
					carId = reservationAncestor.getId().intValue();
				}
			}
			Date startDate = reservationEntity.getTimestamp("startDate").toDate();
			Date endDate = reservationEntity.getTimestamp("endDate").toDate();
			Double rentalPrice = reservationEntity.getDouble("rentalPrice");
			Quote quote = new Quote(renter, startDate, endDate, crcName, carTypeName, rentalPrice);
			Reservation reservation = new Reservation(quote, carId);
			reservations.add(reservation);
		});
    	
    	return reservations;
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
			String carTypeName = carTypeEntity.getKey().getName();
			int nbOfSeats = ((Long) carTypeEntity.getLong("nbOfSeats")).intValue();
			boolean smokingAllowed = carTypeEntity.getBoolean("smokingAllowed");
			double rentalPricePerDay = carTypeEntity.getDouble("rentalPricePerDay");
			float trunkSpace = (float) carTypeEntity.getDouble("trunkSpace");
			CarType cartype = new CarType(carTypeName, nbOfSeats, trunkSpace, rentalPricePerDay, smokingAllowed);
			cartypes.add(cartype);
		});
		
		return cartypes;
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
		String carTypeId = carType.getName();
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
		return this.getReservations(renter).size() > 0;
	}
}
