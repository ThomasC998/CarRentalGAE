package ds.gae.view;

import java.util.stream.Stream;

public enum JSPSite {
	LOGIN("Login", "/login.jsp", false),
	PERSIST_TEST_SERVLET("Persist Test", "/persistTest", true),
	PERSIST_TEST("Persist Test", "/persistTest.jsp", false);

	/**
	 * This is a human readable string describing the JSP site.
	 */
	private String label;

	/**
	 * An absolute URL that this JSP can be reached at
	 */
	private String url;

	/**
	 * Should this site be listed in the navigation bar?
	 */
	private boolean isNavigationItem;

	public String label() {
		return this.label;
	}

	public String url() {
		return this.url;
	}

	JSPSite(String label, String url, boolean isNavigationItem) {
		this.label = label;
		this.url = url;
		this.isNavigationItem = isNavigationItem;
	}

	public static JSPSite[] getNavigationItems() {
		return Stream.of(JSPSite.values()).filter(site -> site.isNavigationItem).toArray(JSPSite[]::new);
	}
}
