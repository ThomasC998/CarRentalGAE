package ds.gae.servlets;

import java.io.IOException;
import java.text.ParseException;
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
import ds.gae.view.JSPSite;
import ds.gae.view.Tools;

@SuppressWarnings("serial")
public class PersistTestServlet extends HttpServlet {

	private static Logger logger = Logger.getLogger(PersistTestServlet.class.getName());

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String companyName = "Hertz";
		String userName = "Pieter A.";

		req.getSession().setAttribute("renter", userName);

		try {

			if (CarRentalModel.get().getReservations(userName).size() == 0) {

				ReservationConstraints c = new ReservationConstraints(Tools.DATE_FORMAT.parse("08.12.2019"),
						Tools.DATE_FORMAT.parse("14.12.2019"), "Compact");

				final Quote q = CarRentalModel.get().createQuote(companyName, userName, c);
				CarRentalModel.get().confirmQuote(q);
			}

			resp.sendRedirect(JSPSite.PERSIST_TEST.url());
		} catch (ParseException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		} catch (ReservationException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
}
