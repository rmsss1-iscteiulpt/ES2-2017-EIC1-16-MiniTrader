package mt.server;

//Alterações feitas
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.mockito.internal.stubbing.answers.ThrowsException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import mt.Order;
import mt.client.controller.Controller;
import mt.comm.ServerComm;
import mt.comm.ServerSideMessage;
import mt.comm.impl.ServerCommImpl;
import mt.exception.ServerException;
import mt.filter.AnalyticsFilter;

/**
 * MicroTraderServer implementation. This class should be responsible to do the
 * business logic of stock transactions between buyers and sellers.
 * 
 * @author Group 78
 *
 */
public class MicroServer implements MicroTraderServer {

	public static void main(String[] args) {
		ServerComm serverComm = new AnalyticsFilter(new ServerCommImpl());
		MicroTraderServer server = new MicroServer();
		server.start(serverComm);
	}

	public static final Logger LOGGER = Logger.getLogger(MicroServer.class.getName());

	/**
	 * Server communication
	 */
	private ServerComm serverComm;

	/**
	 * A map to sore clients and clients orders
	 */
	private Map<String, Set<Order>> orderMap;

	/**
	 * Orders that we must track in order to notify clients
	 */
	private Set<Order> updatedOrders;

	/**
	 * Order Server ID
	 */
	private static int id = 1;

	/** The value is {@value #EMPTY} */
	public static final int EMPTY = 0;

	/**
	 * Constructor
	 */
	public MicroServer() {
		LOGGER.log(Level.INFO, "Creating the server...");
		orderMap = new HashMap<String, Set<Order>>();
		updatedOrders = new HashSet<>();
	}

	@Override
	public void start(ServerComm serverComm) {
		serverComm.start();

		LOGGER.log(Level.INFO, "Starting Server...");

		this.serverComm = serverComm;

		ServerSideMessage msg = null;
		while ((msg = serverComm.getNextMessage()) != null) {
			ServerSideMessage.Type type = msg.getType();

			if (type == null) {
				serverComm.sendError(null, "Type was not recognized");
				continue;
			}

			switch (type) {
			case CONNECTED:
				try {
					processUserConnected(msg);
				} catch (ServerException e) {
					serverComm.sendError(msg.getSenderNickname(), e.getMessage());
				}
				break;
			case DISCONNECTED:
				processUserDisconnected(msg);
				break;
			case NEW_ORDER:
				try {
					verifyUserConnected(msg);
					if (msg.getOrder().getServerOrderID() == EMPTY) {
						msg.getOrder().setServerOrderID(id++);
					}
					boolean send = processNewOrder(msg);
					
					notifyAllClients(msg.getOrder(),send);
				} catch (ServerException e) {
					serverComm.sendError(msg.getSenderNickname(), e.getMessage());
				}
				break;
			default:
				break;
			}
		}
		LOGGER.log(Level.INFO, "Shutting Down Server...");
	}

	/**
	 * Verify if user is already connected
	 * 
	 * @param msg
	 *            the message sent by the client
	 * @throws ServerException
	 *             exception thrown by the server indicating that the user is
	 *             not connected
	 */
	private void verifyUserConnected(ServerSideMessage msg) throws ServerException {
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			if (entry.getKey().equals(msg.getSenderNickname())) {
				return;
			}
		}
		throw new ServerException("The user " + msg.getSenderNickname() + " is not connected.");

	}

	/**
	 * Process the user connection
	 * 
	 * @param msg
	 *            the message sent by the client
	 * 
	 * @throws ServerException
	 *             exception thrown by the server indicating that the user is
	 *             already connected
	 */
	private void processUserConnected(ServerSideMessage msg) throws ServerException {
		LOGGER.log(Level.INFO, "Connecting client " + msg.getSenderNickname() + "...");

		// verify if user is already connected
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			if (entry.getKey().equals(msg.getSenderNickname())) {
				throw new ServerException("The user " + msg.getSenderNickname() + " is already connected.");
			}
		}

		// register the new user
		orderMap.put(msg.getSenderNickname(), new HashSet<Order>());

		notifyClientsOfCurrentActiveOrders(msg);
	}

	/**
	 * Send current active orders sorted by server ID ASC
	 * 
	 * @param msg
	 */
	private void notifyClientsOfCurrentActiveOrders(ServerSideMessage msg) {
		List<Order> ordersToSend = new ArrayList<>();
		// update the new registered user of all active orders
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			Set<Order> orders = entry.getValue();
			for (Order order : orders) {
				ordersToSend.add(order);
			}
		}

		// sort the orders to send to clients by server id
		Collections.sort(ordersToSend, new Comparator<Order>() {
			@Override
			public int compare(Order o1, Order o2) {
				return o1.getServerOrderID() < o2.getServerOrderID() ? -1 : 1;
			}
		});

		for (Order order : ordersToSend) {
			serverComm.sendOrder(msg.getSenderNickname(), order);
		}
	}

	/**
	 * Process the user disconnection
	 * 
	 * @param msg
	 *            the message sent by the client
	 */
	private void processUserDisconnected(ServerSideMessage msg) {
		LOGGER.log(Level.INFO, "Disconnecting client " + msg.getSenderNickname() + "...");

		// remove the client orders
		orderMap.remove(msg.getSenderNickname());

		// notify all clients of current unfulfilled orders
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			Set<Order> orders = entry.getValue();
			for (Order order : orders) {
				serverComm.sendOrder(msg.getSenderNickname(), order);
			}
		}
	}

	/**
	 * Process the new received order
	 * 
	 * @param msg
	 *            the message sent by the client
	 * @return 
	 */
	private boolean processNewOrder(ServerSideMessage msg) throws ServerException {
		LOGGER.log(Level.INFO, "Processing new order...");

		// save the order on map
		if (saveOrder(msg.getOrder())) {
			createXML(msg.getOrder());

			// if is buy order

			// notify clients of changed order
			notifyClientsOfChangedOrders();

			// remove all fulfilled orders
			removeFulfilledOrders();

			// reset the set of changed orders
			updatedOrders = new HashSet<>();
			return true;
		}
		return false;

	}

	/**
	 * Store the order on map
	 * 
	 * @param o
	 *            the order to be stored on map returns false if the Business
	 *            rules and constraints are not met for the AS Region returns
	 *            true if the Business rules and constraints are met for the AS
	 *            Region
	 */
	private boolean saveOrder(Order o) {
		LOGGER.log(Level.INFO, "Storing the new order...");

		if (o.isBuyOrder()) {
			if (o.getNumberOfUnits() < 10) {
				serverComm.sendError(o.getNickname(), "Number of units must be 10 or higher");
				return false;
			} else {
				Set<Order> orders = orderMap.get(o.getNickname());
				orders.add(o);
				processBuy(o);
				return true;
			}
		}

		// if is sell order
		if (o.isSellOrder()) {
			if (o.getNumberOfUnits() < 10) {
				serverComm.sendError(o.getNickname(), "Number of units must be 10 or higher");
				return false;
			} else {
				Set<Order> orders = orderMap.get(o.getNickname());
				orders.add(o);
				processSell(o);
				return true;
			}
		}

		// save order on map

		return false;

	}

	/**
	 * Process the sell order
	 * 
	 * @param sellOrder
	 *            Order sent by the client with a number of units of a stock and
	 *            the price per unit he wants to sell
	 */
	private void processSell(Order sellOrder) {
		LOGGER.log(Level.INFO, "Processing sell order...");

		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			for (Order o : entry.getValue()) {
				if (o.isBuyOrder() && o.getStock().equals(sellOrder.getStock())
						&& o.getPricePerUnit() >= sellOrder.getPricePerUnit()) {
					doTransaction(o, sellOrder);
				}
			}
		}

	}

	/**
	 * Process the buy order
	 * 
	 * @param buyOrder
	 *            Order sent by the client with a number of units of a stock and
	 *            the price per unit he wants to buy
	 */
	private void processBuy(Order buyOrder) {
		LOGGER.log(Level.INFO, "Processing buy order...");

		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			for (Order o : entry.getValue()) {
				if (o.isSellOrder() && buyOrder.getStock().equals(o.getStock())
						&& o.getPricePerUnit() <= buyOrder.getPricePerUnit()) {
					doTransaction(buyOrder, o);
				}
			}
		}

	}

	/**
	 * Process the transaction between buyer and seller
	 * 
	 * @param buyOrder
	 *            Order sent by the client with a number of units of a stock and
	 *            the price per unit he wants to buy
	 * @param sellerOrder
	 *            Order sent by the client with a number of units of a stock and
	 *            the price per unit he wants to sell
	 */
	private void doTransaction(Order buyOrder, Order sellerOrder) {
		LOGGER.log(Level.INFO, "Processing transaction between seller and buyer...");

		if (buyOrder.getNumberOfUnits() >= sellerOrder.getNumberOfUnits()) {
			buyOrder.setNumberOfUnits(buyOrder.getNumberOfUnits() - sellerOrder.getNumberOfUnits());
			sellerOrder.setNumberOfUnits(EMPTY);
		} else {
			sellerOrder.setNumberOfUnits(sellerOrder.getNumberOfUnits() - buyOrder.getNumberOfUnits());
			buyOrder.setNumberOfUnits(EMPTY);
		}

		updatedOrders.add(buyOrder);
		updatedOrders.add(sellerOrder);
	}

	/**
	 * Notifies clients about a changed order
	 * 
	 * @throws ServerException
	 *             exception thrown in the method notifyAllClients, in case
	 *             there's no order
	 */
	private void notifyClientsOfChangedOrders() throws ServerException {
		LOGGER.log(Level.INFO, "Notifying client about the changed order...");
		for (Order order : updatedOrders) {
			notifyAllClients(order,true);
		}
	}

	/**
	 * Notifies all clients about a new order
	 * 
	 * @param order
	 *            refers to a client buy order or a sell order
	 * @throws ServerException
	 *             exception thrown by the server indicating that there is no
	 *             order if any of the Business rules and constraints for the AS
	 *             Region are not met it doesn't notify the clients
	 */
	private void notifyAllClients(Order order, boolean send) throws ServerException {
		LOGGER.log(Level.INFO, "Notifying clients about the new order...");
		if (order == null) {
			throw new ServerException("There was no order in the message");
		}
		if (!send) {
			
		}
		else{
			for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
				serverComm.sendOrder(entry.getKey(), order);
			}
		}
	}

	/**
	 * Remove fulfilled orders
	 */
	private void removeFulfilledOrders() {
		LOGGER.log(Level.INFO, "Removing fulfilled orders...");

		// remove fulfilled orders
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			Iterator<Order> it = entry.getValue().iterator();
			while (it.hasNext()) {
				Order o = it.next();
				if (o.getNumberOfUnits() == EMPTY) {
					it.remove();
				}
			}
		}
	}

	/**
	 * Record order including seller/buyers identification in AS region Add the
	 * order information to a new or already existing
	 * "MicroTraderPersistence.xml" file
	 * 
	 * @param order
	 */
	private void createXML(Order order) {
		try {
//			String s = System.getProperty("user.dir");
			File inputFile = new File("MicroTraderPersistence.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = null;
			Element root = null;
			boolean existe;
			if(inputFile.exists()){
				doc = dBuilder.parse(inputFile);
				root = doc.getDocumentElement();
				existe=true;
				
			}else{
				inputFile.createNewFile();
				doc = dBuilder.newDocument();
				root = doc.createElement("XML");
				existe=false;
			}
			String type;
			if (order.isBuyOrder() == true) {
				type = "BuyOrder";
			} else{
				type = "SellOrder";
			}
			Element newElementOrder = doc.createElement("Order");
			newElementOrder.setAttribute("Id", "" + id);
			newElementOrder.setAttribute("Type", type);
			newElementOrder.setAttribute("Stock", order.getStock());
			newElementOrder.setAttribute("Units", "" + order.getNumberOfUnits());
			newElementOrder.setAttribute("Price", "" + order.getPricePerUnit());
			newElementOrder.setAttribute("Costumer", order.getNickname());
			root.appendChild(newElementOrder);
			if(existe==false){
				doc.appendChild(root);
			}
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			StreamResult result = new StreamResult(new FileOutputStream("MicroTraderPersistence.xml"));
			DOMSource source = new DOMSource(doc);
			transformer.transform(source, result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
