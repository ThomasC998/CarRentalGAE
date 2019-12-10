package ds.gae;

import java.io.Serializable;
import java.util.List;

import ds.gae.entities.Quote;

public class Order implements Serializable {

	String orderId;
	List<Quote> quotes;
	

	public Order(String orderId, List<Quote> quotes) {
		setOrderId(orderId);
		setQuotes(quotes);
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public List<Quote> getQuotes() {
		return quotes;
	}

	public void setQuotes(List<Quote> quotes) {
		this.quotes = quotes;
	}
	
	
}
