<%@page import="java.util.List"%>
<%@page import="ds.gae.CarRentalModel"%>
<%@page import="ds.gae.entities.Reservation"%>
<%@page import="ds.gae.entities.CarType"%>
<%@page import="ds.gae.entities.Reservation"%>
<%@page import="ds.gae.view.JSPSite"%>
<%@page import="ds.gae.view.Tools"%>

<%@page import="java.util.Collection"%>
<%@page import="java.util.HashMap"%>
<%@page import="java.util.Map"%>

<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<% session.setAttribute("currentPage", JSPSite.PERSIST_TEST); %>
<% String renter = (String) session.getAttribute("renter"); %>

<%@include file="_header.jsp"%>

<div class="frameDiv">
	<% Map<String, Integer> carTypes = new HashMap<String, Integer>(); %>
	<% Map<String, Integer> cars = new HashMap<String, Integer>(); %>
	
	<% Collection<String> carRentalCompanies = CarRentalModel.get().getAllRentalCompanyNames(); %>
	
	<% for (String crc : carRentalCompanies) { %>
		<% Collection<CarType> types = CarRentalModel.get().getCarTypesOfCarRentalCompany(crc); %>
		<% carTypes.put(crc, types.size()); %>
		<% int amountOfCars = 0; %>
		
	  	<h2>CarTypes: <%=crc%></h2>
	  	<div class="group">
	  		<table>
	  			<tr>
					<th>Name</th>
					<th class="numbers"># Seats</th>
					<th class="numbers">Trunk</th>
					<th class="numbers">Price Per Day</th>			
					<th>Car Ids</th>
				</tr>
				<% for (CarType t : types ) { %>
					<% amountOfCars += CarRentalModel.get().getAmountOfCarsByCarType(crc, t); %>
					
					<tr>
						<td><%= t.getName() %></td>
						<td class="numbers"><%= t.getNbOfSeats() %></td>
						<td class="numbers"><%= t.getTrunkSpace() %></td>
						<td class="numbers"><%= t.getRentalPricePerDay() %></td>
						<td width="300px" style="text-align: left;">
							<% for (int i : CarRentalModel.get().getCarIdsByCarType(crc, t)) { %>
								<%= i %>
							<% } %>
						</td>
					<tr>
				<% } %>
	  		</table>
	  	</div>
	  	
	  	<% cars.put(crc, amountOfCars); %>
    <% } %>
    
    <h2>Current Reservations</h2>
    <div class="group">
    	<table>
    		<tr>
    			<th>Rental Company</th>	
				<th>Car Type/ID</th>
				<th>Rental Period</th>
				<th>Rental Price</th>	
    		</tr>
    		<% List<Reservation> reservations = CarRentalModel.get().getReservations(renter); %>
    		<% if (reservations.isEmpty()) { %>
    			<tr>
    				<td colspan="4">No Reservations</td>
    			</tr>
    		<% } else { %>
    			<% for (Reservation r : reservations) { %>
	    			<tr>
						<td><%= r.getRentalCompany()%></td>
						<td><%= r.getCarType()%>/<%= r.getCarId()%></td>
						<td><%= Tools.DATE_FORMAT.format(r.getStartDate()) %> - <%= Tools.DATE_FORMAT.format(r.getEndDate())%></td>
						<td class="numbers"><%= r.getRentalPrice()%> €</td>
					</tr>
				<% } %>
    		<% } %>
    	</table>
    </div>
    
    <h2>Summary</h2>
    <div class="group">
    	<table>
    		<tr>
    			<th>CRC</th>
				<th># Car Types</th>
				<th># Cars</th>
    		</tr>
    		<% for (String crc : carRentalCompanies) { %>
    			<tr>
					<td><%= crc %></td>
					<td><%= carTypes.get(crc) %> / 7</td>
					<td><%= cars.get(crc) %> / <%= crc.equals("Hertz") ? 78 : 46 %></td>
				</tr>
    		<% } %>
    	</table>
    	<p class="stress">
    		Reservations: <%= reservations.size() %>
    	</p>
    </div>
</div>

<%@include file="_footer.jsp"%>
