package ds.gae;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;

import ds.gae.entities.Car;
import ds.gae.entities.CarRentalCompany;
import ds.gae.entities.CarType;

// This class handles all interaction between the DataStore and the CarRentalModel
public class DataStoreHandler {
	
	private static Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
	
	private static KeyFactory crcKeyFactory = datastore.newKeyFactory().setKind("crc");
	private static KeyFactory carTypeKeyFactory = datastore.newKeyFactory().setKind("cartype");
	private static KeyFactory carKeyFactory = datastore.newKeyFactory().setKind("car");
	private static KeyFactory reservationKeyFactory = datastore.newKeyFactory().setKind("reservation");
	
	public DataStoreHandler() {
		
	}
	
	public static void storeCRC(CarRentalCompany crc) {
		Key key = crcKeyFactory.newKey(crc.getName());
		
		Entity crcEntity = Entity.newBuilder(key)
				//.set("name", crc.getName())
			    .build();
		datastore.put(crcEntity);
	}
	
	public static void storeCarType(CarType ct, String crcName) {
		String carTypeId = ct.getName() + crcName;
		Key key = carTypeKeyFactory
				.addAncestor(PathElement.of("crc", crcName))
				.setKind("cartype")
				.newKey(ct.getName());
		
		Entity ctEntity = Entity.newBuilder(key)
				.set("name", ct.getName())
				.set("nbOfSeats", ct.getNbOfSeats())
				.set("smokingAllowed",ct.isSmokingAllowed())
				.set("rentalPricePerDay", ct.getRentalPricePerDay())
				.set("trunkSpace", ct.getTrunkSpace())
				.build();
		datastore.put(ctEntity);
	}
	
	public static void storeCar(Car car, String crcName) {
		String carTypeId = car.getType().getName() + crcName;
		Key key = carKeyFactory
				.addAncestor(PathElement.of("cartype", carTypeId))
				.setKind("car")
				.newKey(car.getId());
		
		Entity carEntity = Entity.newBuilder(key)
				.build();
		datastore.put(carEntity);
	}
	
}
