package ds.gae.entities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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

import ds.gae.ReservationException;

public class CarRentalCompany {

	private static Logger logger = Logger.getLogger(CarRentalCompany.class.getName());

	// key
	private String name;
	// children
	private Set<Car> cars;
	// children
	private Map<String, CarType> carTypes = new HashMap<String, CarType>();

	/***************
	 * CONSTRUCTOR *
	 ***************/

	public CarRentalCompany(String name, Set<Car> cars) {
		setName(name);
		this.cars = cars;
		for(Car car : cars) {
			carTypes.put(car.getType().getName(), car.getType());
		}
	}

	/********
	 * NAME *
	 ********/

	public String getName() {
		return name;
	}

	private void setName(String name) {
		this.name = name;
	}

	/*************
	 * CAR TYPES *
	 *************/

	public Collection<CarType> getAllCarTypes() {
		return carTypes.values();
	}

	public CarType getCarType(String carTypeName) {
		return carTypes.get(carTypeName);
	}

	public boolean isAvailable(String carTypeName, Date start, Date end) {
		logger.log(Level.INFO, "<{0}> Checking availability for car type {1}", new Object[] { name, carTypeName });
		boolean isAvailable = false;
		for (CarType carType: getAvailableCarTypes(start, end)) {
			if (carType.getName().equals(carTypeName)) {
				isAvailable = true;
			}
		}
		return isAvailable;
	}

	public Set<CarType> getAvailableCarTypes(Date start, Date end) {
		Set<CarType> availableCarTypes = new HashSet<CarType>();
		
		// get carType of all available cars
		List<Car> availableCars = getAllAvailableCars(start, end);
		for (Car car: availableCars) {
			availableCarTypes.add(car.getType());
		}
		
		return availableCarTypes;
	}
	
	private List<Car> getAllAvailableCars(Date start, Date end) {
		String carType = "";
		return getAvailableCars(carType, start, end);
	}

	/*********
	 * CARS *
	 *********/

	private Car getCar(int uid) {
		for (Car car : cars) {
			if (car.getId() == uid) {
				return car;
			}
		}
		throw new IllegalArgumentException("<" + name + "> No car with uid " + uid);
	}

	public Set<Car> getCars() {
		return cars;
	}
	
	/**
	 * Get entities of all reservations from datastore
	 * @param start
	 * @param end
	 * @return
	 */
	private List<Entity> getReservationEntitiesInDateRange(Date start, Date end) {
		
		// get all cars that are reserved in the given period
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		List<Entity> reservationEntities = new ArrayList<Entity>();
		
		// get reservations of which the startDate falls in between start and end
		Query<Entity> query = Query.newEntityQueryBuilder()
				.setKind("reservation")
				.setFilter(CompositeFilter.and(
						PropertyFilter.ge("startDate", Timestamp.of(start)),
						PropertyFilter.le("startDate", Timestamp.of(end))))
				.build();
		QueryResults<Entity> reservationsInCurrentDateRangeEntities = datastore.run(query);
		
		reservationsInCurrentDateRangeEntities.forEachRemaining( resEntity -> {
			reservationEntities.add(resEntity);
		});
		
		// get reservations of which the endDate falls in between start and end
		Query<Entity> reservationQuery2 = Query.newEntityQueryBuilder()
				.setKind("reservation")
				.setFilter(CompositeFilter.and(
						PropertyFilter.ge("endDate", Timestamp.of(start)),
						PropertyFilter.le("endDate", Timestamp.of(end))))
				.build();
		QueryResults<Entity> reservationsInCurrentDateRangeEntities2 = datastore.run(reservationQuery2);
		
		
		reservationsInCurrentDateRangeEntities2.forEachRemaining( resEntity -> {
			boolean addEntity = true;
			for (Entity resEntity1: reservationEntities) {
				if(resEntity.getKey().getId().equals(resEntity1.getKey().getId())) {
					addEntity = false;
				}
			}
			if(addEntity) {
				reservationEntities.add(resEntity);
			}
		});
		
		return reservationEntities;
	}
	
	/**
	 * Get all cars of this CarRentalCompany and of the given carType
	 * that are reserved in (partially) in the date range from the given
	 * start until the given end.
	 * 
	 * @param carType
	 * @param start
	 * @param end
	 * @return List<Car>
	 */
	private List<Car> getReservedCars(String carType, Date start, Date end) {
		
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		// get all current reservations
		List<Entity> reservationEntities = getReservationEntitiesInDateRange(start, end);
		
		// get cars of the current reservations
		List<Car> carsInReservation = new ArrayList<Car>();
		for (Entity reservationEntity: reservationEntities) {
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
			if ((carType.equals(carTypeName) || carType.equals("")) && (this.getName().equals(crcName))) {
				Key carTypeKey = datastore.newKeyFactory()
						.addAncestor(PathElement.of("crc", crcName))
						.setKind("cartype")
						.newKey(carTypeName);
				Entity carTypeEntity = datastore.get(carTypeKey);
				int nbOfSeats = ((Long) carTypeEntity.getLong("nbOfSeats")).intValue();
				boolean smokingAllowed = carTypeEntity.getBoolean("smokingAllowed");
				double rentalPricePerDay = carTypeEntity.getDouble("rentalPricePerDay");
				float trunkSpace = (float) carTypeEntity.getDouble("trunkSpace");
				CarType cartype = new CarType(carTypeName, nbOfSeats, trunkSpace, rentalPricePerDay, smokingAllowed);
				Car car = new Car(carId, cartype);
				car.setRentalCompany(crcName);
				carsInReservation.add(car);
			}
		}
		return carsInReservation;
	}
	
	/**
	 * Get all cars of this CarRentalCompany and of the given carType.
	 * 
	 * @param carType
	 * @return List<Car>
	 */
	private List<Car> getAllCars(String carType) {
		
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		Query<Entity> query2 = Query.newEntityQueryBuilder()
				.setKind("car")
				.build();
		QueryResults<Entity> carEntities = datastore.run(query2);
		
		List<Car> allCars = new ArrayList<Car>(); 
		carEntities.forEachRemaining( carEntity -> {
			List<PathElement> carAncestors = carEntity.getKey().getAncestors();
			String crcName = "";
			String carTypeName = "";
			int carId = ((Long) carEntity.getKey().getId()).intValue();
			for (PathElement carAncestor: carAncestors) {
				if(carAncestor.getKind().equals("crc")) {
					crcName = carAncestor.getName();
				} else if(carAncestor.getKind().equals("cartype")) {
					carTypeName = carAncestor.getName();
				}
			}
			if ((carType.equals(carTypeName) || carType.equals("")) && (this.getName().equals(crcName))) {
				Key carTypeKey = datastore.newKeyFactory()
						.addAncestor(PathElement.of("crc", crcName))
						.setKind("cartype")
						.newKey(carTypeName);
				Entity carTypeEntity = datastore.get(carTypeKey);
				int nbOfSeats = ((Long) carTypeEntity.getLong("nbOfSeats")).intValue();
				boolean smokingAllowed = carTypeEntity.getBoolean("smokingAllowed");
				double rentalPricePerDay = carTypeEntity.getDouble("rentalPricePerDay");
				float trunkSpace = (float) carTypeEntity.getDouble("trunkSpace");
				CarType cartype = new CarType(carTypeName, nbOfSeats, trunkSpace, rentalPricePerDay, smokingAllowed);
				Car car = new Car(carId, cartype);
				car.setRentalCompany(crcName);
				allCars.add(car);
			}
		});
		
		return allCars;
	}

	/**
	 * Get all cars of this CarRentalCompany and of the given carType
	 * that are available in the date range from the given
	 * start until the given end.
	 * 
	 * @param carType
	 * @param start
	 * @param end
	 * @return List<Car>
	 */
	private List<Car> getAvailableCars(String carType, Date start, Date end) {
		List<Car> availableCars = new ArrayList<Car>();
		
		// get reserved cars
		List<Car> reservedCars = getReservedCars(carType, start, end);
		
		// get all cars (available and reserved)
		List<Car> allCars = getAllCars(carType);
		
		// available cars are cars that are not reserved in the given period
		for (Car car: allCars) {
			boolean carIsAvailable = true;
			for (Car reservedCar: reservedCars) {
				if (reservedCar.getId() == car.getId() && reservedCar.getRentalCompanyName().equals(car.getRentalCompanyName())) {
					carIsAvailable = false;
				}
			}
			if (carIsAvailable) {
				availableCars.add(car);
			}
		}
		
		return availableCars;
	}

	/****************
	 * RESERVATIONS *
	 ****************/

	public Quote createQuote(ReservationConstraints constraints, String client) throws ReservationException {
		logger.log(Level.INFO, "<{0}> Creating tentative reservation for {1} with constraints {2}",
				new Object[] { name, client, constraints.toString() });
		
		if (!isAvailable(constraints.getCarType(), constraints.getStartDate(), constraints.getEndDate())) {
			throw new ReservationException("<" + name + "> No cars available to satisfy the given constraints.");
		}
		
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		String carTypeId = constraints.getCarType();
		Key carTypeKey = datastore.newKeyFactory()
				.addAncestor(PathElement.of("crc", this.getName()))
				.setKind("cartype")
				.newKey(carTypeId);
		Entity carTypeEntity = datastore.get(carTypeKey);
		Double rentalPricePerDay = carTypeEntity.getDouble("rentalPricePerDay");

		double price = calculateRentalPrice(
				rentalPricePerDay,
				constraints.getStartDate(),
				constraints.getEndDate()
		);

		return new Quote(
				client,
				constraints.getStartDate(),
				constraints.getEndDate(),
				getName(),
				constraints.getCarType(),
				price
		);
	}

	// Implementation can be subject to different pricing strategies
	private double calculateRentalPrice(double rentalPricePerDay, Date start, Date end) {
		return rentalPricePerDay * Math.ceil((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24D));
	}

	public Reservation confirmQuote(Quote quote) throws ReservationException {
		logger.log(Level.INFO, "<{0}> Reservation of {1}", new Object[] { name, quote.toString() });
		List<Car> availableCars = getAvailableCars(quote.getCarType(), quote.getStartDate(), quote.getEndDate());
		if (availableCars.isEmpty()) {
			throw new ReservationException("Reservation failed, all cars of type " + quote.getCarType()
					+ " are unavailable from " + quote.getStartDate() + " to " + quote.getEndDate());
		}
		Car car = availableCars.get((int) (Math.random() * availableCars.size()));

		Reservation res = new Reservation(quote, car.getId());
		car.addReservation(res);
		
		// use datastore
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
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
		
		datastore.put(reservationEntity);
		
		return res;
	}
	
	public Car getAvailableCar(Quote quote, List<Reservation> currentReservations) throws ReservationException {
		logger.log(Level.INFO, "<{0}> Reservation of {1}", new Object[] { name, quote.toString() });
		List<Car> availableCars = getAvailableCars(quote.getCarType(), quote.getStartDate(), quote.getEndDate());
		if (availableCars.isEmpty()) {
			throw new ReservationException("Reservation failed, all cars of type " + quote.getCarType()
					+ " are unavailable from " + quote.getStartDate() + " to " + quote.getEndDate());
		}

		// don't pick a car randomly, check the list of currentReservations that are already
		// confirmed in the transaction
		Car car = null;
		boolean carAvailable = false;
		for (Car availableCar: availableCars) {
			boolean contains = false;
			for (Reservation res: currentReservations) {
				if (res.getCarId() == availableCar.getId() && res.getRentalCompany().equals(availableCar.getRentalCompanyName())) {
					contains = true;
				}
			}
			if (!contains) {
				car = availableCar;
				carAvailable = true;
				break;
			}
		}
		if (!carAvailable) {
			throw new ReservationException("Reservation failed, all cars of type " + quote.getCarType()
			+ " are unavailable from " + quote.getStartDate() + " to " + quote.getEndDate());
		}
		
		return car;
	}

	public void cancelReservation(Reservation res) {
		logger.log(Level.INFO, "<{0}> Cancelling reservation {1}", new Object[] { name, res.toString() });
		getCar(res.getCarId()).removeReservation(res);
	}
}
