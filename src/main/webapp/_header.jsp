<%@page import="ds.gae.view.JSPSite"%>

<% JSPSite currentPage = (JSPSite) session.getAttribute("currentPage"); %>

<!DOCTYPE html>
<html>
<head>
<link rel="stylesheet" type="text/css" href="style.css" />
<title>Car Rental Application</title>
</head>
<body>
	<div id="mainWrapper">
		<div id="headerWrapper">
			<h1>Car Rental Application</h1>
		</div>

		<div id="navigationWrapper">
			<ul>
				<% for (JSPSite site : JSPSite.getNavigationItems()) { %>
					<li>
						<a
							class="<%=site == session.getAttribute("currentPage") ? "selected" : ""%>"
							href="<%=site.url()%>"><%=site.label()%>
						</a>
					</li>
				<% } %>
			</ul>
		</div>

		<div id="contentWrapper">
			<% if (currentPage != JSPSite.LOGIN && currentPage != JSPSite.PERSIST_TEST) { %>
				<div id="userProfile">
					<span>Logged-in as <%=session.getAttribute("renter")%> (<a href="/login.jsp">change</a>)</span>
				</div>
			<% } %>