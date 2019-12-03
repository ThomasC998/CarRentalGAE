<%@page import="ds.gae.view.JSPSite"%>
<%@page import="ds.gae.CarRentalModel"%>

<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<% session.setAttribute("currentPage", JSPSite.RESERVATION_ERROR); %>
<% String renter = (String) session.getAttribute("renter"); %>

<%@include file="_header.jsp"%>

<div class="frameDiv" style="margin: 150px 280px;">
	<h2>Reservation Error</h2>
	<div class="group">
		<div class="form">
			<span><%= (String)session.getAttribute("errorMsg") %></span>
		</div>
	</div>
</div>

<%@include file="_footer.jsp"%>
