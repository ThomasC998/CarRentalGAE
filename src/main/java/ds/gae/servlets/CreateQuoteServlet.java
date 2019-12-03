package ds.gae.servlets;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import ds.gae.CarRentalModel;
import ds.gae.ReservationException;
import ds.gae.entities.Quote;
import ds.gae.entities.ReservationConstraints;
import ds.gae.view.Tools;
import ds.gae.view.JSPSite;

@SuppressWarnings("serial")
public class CreateQuoteServlet extends HttpServlet {

	private static Logger logger = Logger.getLogger(CreateQuoteServlet.class.getName());

	@SuppressWarnings("unchecked")
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			Date startDate = Tools.DATE_FORMAT.parse(req.getParameter("startDate"));
			Date endDate = Tools.DATE_FORMAT.parse(req.getParameter("endDate"));
			String carType = req.getParameter("carType");
			String crc = req.getParameter("crc");
			String renter = (String) req.getSession().getAttribute("renter");

			Quote q = CarRentalModel.get().createQuote(crc, renter,
					new ReservationConstraints(startDate, endDate, carType));

			HashMap<String, ArrayList<Quote>> quotes = (HashMap<String, ArrayList<Quote>>) req.getSession()
					.getAttribute("quotes");
			if (quotes == null) {
				quotes = new HashMap<String, ArrayList<Quote>>();
				req.setAttribute("quotes", quotes);
			}

			ArrayList<Quote> quotesOfCurrentCrc;
			if (quotes.containsKey(crc)) {
				quotesOfCurrentCrc = quotes.get(crc);
			} else {
				quotesOfCurrentCrc = new ArrayList<Quote>();
				quotes.put(crc, quotesOfCurrentCrc);
			}

			quotesOfCurrentCrc.add(q);
			req.getSession().setAttribute("quotes", quotes);

			resp.sendRedirect(JSPSite.CREATE_QUOTES.url());
		} catch (ParseException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		} catch (ReservationException e) {
			req.getSession().setAttribute("errorMsg", Tools.encodeHTML(e.getMessage()));
			resp.sendRedirect(JSPSite.RESERVATION_ERROR.url());
		}
	}
}
