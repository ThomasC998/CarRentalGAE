package ds.gae.entities;

import java.util.Date;
import java.util.Objects;

public class Reservation extends Quote {
	
	private int carId;

	/***************
	 * CONSTRUCTOR *
	 ***************/

	public Reservation(Quote quote, int carId) {
		this( 
				quote.getRenter(),
				quote.getStartDate(),
				quote.getEndDate(),
				quote.getRentalCompany(),
				quote.getCarType(),
				quote.getRentalPrice()
		);
		this.carId = carId;
	}

	private Reservation(
			String renter,
			Date start,
			Date end,
			String rentalCompany,
			String carType,
			double rentalPrice) {
		super(renter, start, end, rentalCompany, carType, rentalPrice);
	}

	/******
	 * ID *
	 ******/

	public int getCarId() {
		return carId;
	}

	/*************
	 * TO STRING *
	 *************/

	@Override
	public String toString() {
		return String.format(
				"Reservation for %s from %s to %s at %s\nCar type: %s\tCar: %s\nTotal price: %.2f",
				getRenter(),
				getStartDate(),
				getEndDate(),
				getRentalCompany(),
				getCarType(),
				getCarId(),
				getRentalPrice()
		);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), getCarId());
	}

	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj)) {
			return false;
		}
		Reservation other = (Reservation) obj;
		if (getCarId() != other.getCarId()) {
			return false;
		}
		return true;
	}
}
