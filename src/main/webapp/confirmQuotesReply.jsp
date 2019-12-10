<%@page import="ds.gae.view.JSPSite"%>

<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<% session.setAttribute("currentPage", JSPSite.CONFIRM_QUOTES_RESPONSE); %>
<% String renter = (String) session.getAttribute("renter"); %>

<%@include file="_header.jsp"%>

<div class="frameDiv" style="margin: 150px 150px;">
	<h2>Your reservations have been succesfully received!</h2>
	<div class="group">
		<p>
			The reservations for <%=renter%> are being processed.
			You will receive a mail when they are confirmed or when something went wrong.
		</p>
	</div>
</div>

<%@include file="_footer.jsp"%>
