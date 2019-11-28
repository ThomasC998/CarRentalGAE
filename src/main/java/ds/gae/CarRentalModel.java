package ds.gae;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ds.gae.entities.Car;
import ds.gae.entities.CarRentalCompany;
import ds.gae.entities.CarType;
import ds.gae.entities.Quote;
import ds.gae.entities.Reservation;
import ds.gae.entities.ReservationConstraints;

public class CarRentalModel {

	// FIXME use persistence instead
	public Map<String,CarRentalCompany> CRCS = new HashMap<String, CarRentalCompany>();	

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
		// FIXME add implementation
		return null;
	}

	/**
	 * Get the names of all registered car rental companies
	 *
	 * @return the list of car rental companies
	 */
	public Collection<String> getAllRentalCompanyNames() {
		// FIXME use persistence instead
    	return CRCS.keySet();
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
    	CarRentalCompany crc = CRCS.get(companyName);
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
		CarRentalCompany crc = CRCS.get(quote.getRentalCompany());
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
	 * Get all reservations made by the given car renter.
	 *
	 * @param renter name of the car renter
	 * @return the list of reservations of the given car renter
	 */
	public List<Reservation> getReservations(String renter) {
		// FIXME: use persistence instead
		List<Reservation> out = new ArrayList<Reservation>();	
    	for (CarRentalCompany crc : CRCS.values()) {
    		for (Car c : crc.getCars()) {
    			for (Reservation r : c.getReservations()) {
    				if (r.getRenter().equals(renter)) {
    					out.add(r);
    				}
    			}
    		}
    	}
    	return out;
	}

	/**
	 * Get the car types available in the given car rental company.
	 *
	 * @param companyName the given car rental company
	 * @return The list of car types in the given car rental company.
	 */
	public Collection<CarType> getCarTypesOfCarRentalCompany(String companyName) {
		// FIXME: use persistence instead
    	CarRentalCompany crc = CRCS.get(companyName);
    	Collection<CarType> out = new ArrayList<CarType>(crc.getAllCarTypes());
        return out;
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
		// FIXME: use persistence instead
		List<Car> out = new ArrayList<Car>(); 
		for(CarRentalCompany crc : CRCS.values()) {
			for (Car c : crc.getCars()) {
				if (c.getType() == carType) { 
					out.add(c);
				}
			}
		}
		return out;

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
