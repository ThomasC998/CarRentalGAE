package ds.gae.entities;

import java.util.Objects;

public class CarType {

	private String name;
	private int nbOfSeats;
	private boolean smokingAllowed;
	private double rentalPricePerDay;
	// trunk space in liters
	private float trunkSpace;

	/***************
	 * CONSTRUCTOR *
	 ***************/

	public CarType(
			String name,
			int nbOfSeats,
			float trunkSpace,
			double rentalPricePerDay,
			boolean smokingAllowed) {
		this.name = name;
		this.nbOfSeats = nbOfSeats;
		this.trunkSpace = trunkSpace;
		this.rentalPricePerDay = rentalPricePerDay;
		this.smokingAllowed = smokingAllowed;
	}

	public String getName() {
		return name;
	}

	public int getNbOfSeats() {
		return nbOfSeats;
	}

	public boolean isSmokingAllowed() {
		return smokingAllowed;
	}

	public double getRentalPricePerDay() {
		return rentalPricePerDay;
	}

	public float getTrunkSpace() {
		return trunkSpace;
	}

	/*************
	 * TO STRING *
	 *************/

	@Override
	public String toString() {
		return String.format(
				"Car type: %s \t[seats: %d, price: %.2f, smoking: %b, trunk: %.0fl]",
				getName(),
				getNbOfSeats(),
				getRentalPricePerDay(),
				isSmokingAllowed(),
				getTrunkSpace()
		);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		CarType other = (CarType) obj;
		if (!Objects.equals(name, other.name)) {
			return false;
		}
		return true;
	}
}
