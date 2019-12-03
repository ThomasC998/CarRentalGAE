<%@page import="java.util.HashMap"%>
<%@page import="java.util.List"%>
<%@page import="ds.gae.view.JSPSite"%>
<%@page import="ds.gae.view.Tools"%>
<%@page import="ds.gae.CarRentalModel"%>
<%@page import="ds.gae.entities.Quote"%>

<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<% session.setAttribute("currentPage", JSPSite.CREATE_QUOTES); %>
<% String renter = (String) session.getAttribute("renter"); %>
<% boolean hasQuotes = false; %>

<%@include file="_header.jsp"%>

<div class="frameDiv">
	<% for (String crc : CarRentalModel.get().getAllRentalCompanyNames()) { %>
		<h2>Car Rental Company: <%= crc %></h2>
		
		<h3>Create a Quote</h3>
		<form method="POST" action="/createQuote">
			<div class="group">
				<div class="form">
					<span>From: <input type="text" name="startDate" value="01.12.2019" size="10"> (dd.mm.yyyy)</span>
					<span>To: <input type="text" name="endDate" value="01.01.2020" size="10"> (dd.mm.yyyy)</span>
				</div>
				<div class="form">
					<span>
						Car Type:
						<select name="carType">
							<% for (String carTypeName : CarRentalModel.get().getCarTypesNames(crc)) { %>
								<option><%= carTypeName %></option>
							<% } %>
						</select>
					</span>
				</div>
				<div class="formsubmit">
					<input type="hidden" name="crc" value="<%= crc %>"/>
					<input type="submit" value="Create" />
				</div>
			</div>
		</form>
		
		<% HashMap<String, List<Quote>> quotes = (HashMap<String, List<Quote>>) session.getAttribute("quotes"); %>
		<% if (quotes != null && quotes.containsKey(crc) && quotes.get(crc).size() > 0) { %>
			<% hasQuotes = true; %>
			<% List<Quote> quotesForCrc = quotes.get(crc); %>
			<h3>Current Quotes</h3>
			<div class="group">
				<table>
					<tr>
						<th>Car Type</th>
						<th>Start Date</th>
						<th>End Date</th>
						<th class="numbers">Rental Price</th>			
					</tr>
					<% for (Quote q : quotesForCrc) { %>
						<tr>
							<td><%= q.getCarType()%></td>
							<td><%= Tools.DATE_FORMAT.format(q.getStartDate())%></td>
							<td><%= Tools.DATE_FORMAT.format(q.getEndDate())%></td>
							<td class="numbers"><%= q.getRentalPrice()%> €</td>
						</tr>
					<% } %>
				</table>
			</div>
		<% } %>
	<% } %>
	
	<% if (hasQuotes) { %>
		<h2>Confirmation of Quotes</h2>
		<div class="formsubmit">
			<form method="POST" action="/confirmQuotes">
				<input id="confirmSubmitButton" type="submit" value=" >> Confirm all Quotes<< " />
			</form>
		</div>
	<% } %>
</div>

<%@include file="_footer.jsp"%>
