package ds.gae.entities;

import java.util.Date;
import java.util.Objects;

public class ReservationConstraints {

	private Date startDate;
	private Date endDate;
	private String carType;

	public ReservationConstraints(Date start, Date end, String carType) {
		setStartDate(start);
		setEndDate(end);
		setCarType(carType);
	}

	public Date getStartDate() {
		return startDate;
	}

	private void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	private void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public String getCarType() {
		return carType;
	}

	private void setCarType(String carType) {
		this.carType = carType;
	}

	@Override
	public String toString() {
		return String.format(
				"Reservation constraints [from %s until %s, for car type '%s']",
				getStartDate(),
				getEndDate(),
				getCarType());
	}

	@Override
	public int hashCode() {
		return Objects.hash(endDate, startDate, carType);
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
		ReservationConstraints other = (ReservationConstraints) obj;
		if (!Objects.equals(endDate, other.endDate)) {
			return false;
		}
		if (!Objects.equals(startDate, other.startDate)) {
			return false;
		}
		if (!Objects.equals(carType, other.carType)) {
			return false;
		}
		return true;
	}
}
