package ds.gae;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Transaction;

import ds.gae.entities.Car;
import ds.gae.entities.CarRentalCompany;
import ds.gae.entities.Quote;
import ds.gae.entities.Reservation;

@SuppressWarnings("serial")
public class Worker extends HttpServlet {

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		try {
			// Retrieve the quotes list object from the request
			ObjectInputStream ois = new ObjectInputStream(req.getInputStream());
			Order order = (Order) ois.readObject();
			List<Quote> quotes = order.getQuotes();
			String renter = quotes.get(0).getRenter();
			
			try {
				// confirm the quotes
				List<Reservation> confirmedReservations = confirmQuotes(quotes);
				
				// send mail
				System.out.println();
				System.out.println("Mail to: " + renter);
				System.out.println("Subject: Reservations for orderId: " + order.getOrderId() +  " confirmed!");
				System.out.println("Body: ");
				System.out.println("--- Your reservations have been successfully confirmed!");
				System.out.println("--- Reservations for user " + renter + ":");
				for(int i = 1; i <= confirmedReservations.size();i++) {
					System.out.println("--- " + i + ") " + confirmedReservations.get(i-1));
				}
				// Task succeeded
				resp.setStatus(200);
			}catch(ReservationException e) {
				resp.setStatus(200); // Don't retry this task if a reservation exception is thrown
				// send mail
				System.out.println();
				System.out.println("Mail to: " + renter);
				System.out.println("Subject: Reservations for orderId: " + order.getOrderId() +  " failed!");
				System.out.println("Body: ");
				System.out.println("--- Your given reservation constraints are not possible!");
			}
		} catch(ClassNotFoundException e) {
			resp.setStatus(405);
		}
	}
	
	
	private List<Reservation> confirmQuotes(List<Quote> quotes) throws ReservationException {
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		Transaction tx = datastore.newTransaction();
		
		List<Reservation> reservations = new ArrayList<Reservation>();
		try {
			for (Quote quote: quotes) {
				Reservation reservation = confirmQuoteInTransaction(quote, tx, reservations);
//				for (Reservation alreadyConfirmedReservation: reservations) {
//					if (alreadyConfirmedReservation.getCarId() == reservation.getCarId()
//							&& alreadyConfirmedReservation.getRentalCompany().equals(reservation.getRentalCompany())) {
//						throw new ReservationException("Reservation failed, this renter tried to reserve the car with id "
//							+ reservation.getCarId() + " of rental company " + reservation.getRentalCompany() + " more than once.");
//					}
//				}
				reservations.add(reservation);
			}
			tx.commit();
		} finally {
			//System.out.println("finally");
			if (tx.isActive()) {
				System.out.println("rollback");
				tx.rollback();
			}
		}
    	return reservations;
	}
	
	
	private Reservation confirmQuoteInTransaction(Quote quote, Transaction tx, List<Reservation> reservations) throws ReservationException {
		
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		
		Key crcKey = datastore.newKeyFactory()
				.setKind("crc")
				.newKey(quote.getRentalCompany());
		
		Entity crcEntity = datastore.get(crcKey);
		
		Set<Car> cars = new HashSet<Car>();
		CarRentalCompany crc = new CarRentalCompany(crcEntity.getKey().getName(), cars);
		
		System.out.println("getAvailableCar in confirmQuoteInTransaction");
		Car car = crc.getAvailableCar(quote, reservations);
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
		//System.out.println("reservations");
		//System.out.println(getReservations(quote.getRenter()));
		
		return res;
	}
	
}
