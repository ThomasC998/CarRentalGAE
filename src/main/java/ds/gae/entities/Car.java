package ds.gae.entities;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class Car {

	// key
	private int id;
	// child of
	private CarType carType;
	//children
	private Set<Reservation> reservations;

	/***************
	 * CONSTRUCTOR *
	 ***************/

	public Car(int uid, CarType carType) {
		this.id = uid;
		this.carType = carType;
		this.reservations = new HashSet<Reservation>();
	}

	/******
	 * ID *
	 ******/

	public int getId() {
		return id;
	}

	/************
	 * CAR TYPE *
	 ************/

	public CarType getType() {
		return carType;
	}

	/****************
	 * RESERVATIONS *
	 ****************/

	public Set<Reservation> getReservations() {
		return reservations;
	}

	public boolean isAvailable(Date start, Date end) {
		if (!start.before(end)) {
			throw new IllegalArgumentException("Illegal given period");
		}

		for (Reservation reservation : getReservations()) {
			if (reservation.getEndDate().before(start) || reservation.getStartDate().after(end)) {
				continue;
			}
			return false;
		}
		return true;
	}

	public void addReservation(Reservation res) {
		reservations.add(res);
	}

	public void removeReservation(Reservation reservation) {
		reservations.remove(reservation);
	}
}
