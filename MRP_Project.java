
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Mrp_Proje {

	public static void main(String[] args) throws SQLException {

		Connection conn = null;
		try {

			String url = "jdbc:sqlite:C:/Users/Elif Erden/Desktop/MRP.db";
			conn = DriverManager.getConnection(url);
			System.out.println("Connection to SQLite has been established.");

		} 
		catch (SQLException e) {
			System.out.println(e.getMessage());
		}

		Statement stmt = conn.createStatement();
		ResultSet rs;
		
		// Creating PARTS table
		String create_parts = "CREATE TABLE IF NOT EXISTS parts (\n" + " part_id integer PRIMARY KEY,\n"
				+ " part_name text\n" + ");";

		stmt.executeUpdate(create_parts);

		// Creating ORDERS table
		String create_orders = "CREATE TABLE IF NOT EXISTS orders (\n" + " order_id integer PRIMARY KEY,\n"
				+ " product_id integer,\n" + " order_quantity integer,\n" + " order_period integer,\n"
				+ " Constraint index1 FOREIGN KEY(product_id) references PARTS(part_id)\n" + ");";

		stmt.executeUpdate(create_orders);

		// Creating COMPONENTS table
		String create_components = "CREATE TABLE IF NOT EXISTS components (\n" + " component_id integer PRIMARY KEY,\n"
				+ " component_name text,\n" + " BOM_level integer,\n" + " quantity integer,\n"
				+ " inventory_level integer,\n" + " lead_time integer,\n" + " parent_id integer,\n"
				+ " Constraint index2 FOREIGN KEY(parent_id) references PARTS(part_id)\n" + ");";

		stmt.executeUpdate(create_components);

		// Creating GROSS REQUIREMENTS table
		String create_gross = "CREATE TABLE IF NOT EXISTS gross_requirements(\n" + " part_id integer,\n"
				+ " period integer,\n" + " gross_requirement integer\n" + ");";

		stmt.executeUpdate(create_gross);

		// Creating MRP table
		String create_mrp = "CREATE TABLE IF NOT EXISTS MRP (\n" + " part_id integer,\n" + " period integer,\n"
				+ " gross_requirement integer,\n" + " projected_balance integer,\n" + " net_requirement integer,\n"
				+ " order_release integer,\n" + " order_receipt integer\n" + ");";

		stmt.executeUpdate(create_mrp);

		System.out.println("Created tables in given database...");

		stmt.executeUpdate("INSERT INTO parts VALUES ('1', 'A')");
		stmt.executeUpdate("INSERT INTO parts VALUES ('2', 'B')");
		stmt.executeUpdate("INSERT INTO parts VALUES ('3', 'C')");
		stmt.executeUpdate("INSERT INTO parts VALUES ('4', 'D')");
		stmt.executeUpdate("INSERT INTO parts VALUES ('5', 'E')");

		stmt.executeUpdate("INSERT INTO orders VALUES ('1', '1', '5', '2')");
		stmt.executeUpdate("INSERT INTO orders VALUES ('2', '2', '10', '4')");

		stmt.executeUpdate("INSERT INTO components VALUES ('3', 'C', '1', '2', '5', '1', '1')");
		stmt.executeUpdate("INSERT INTO components VALUES ('4', 'D', '1', '1', '0', '2', '1')");
		stmt.executeUpdate("INSERT INTO components VALUES ('5', 'E', '1', '2', '0', '2', '2')");
		stmt.executeUpdate("INSERT INTO components VALUES ('6', 'F', '2', '2', '0', '1', '4')");
		stmt.executeUpdate("INSERT INTO components VALUES ('7', 'G', '2', '1', '0', '1', '4')");

		stmt.executeUpdate("INSERT INTO gross_requirements VALUES ('1', '1', '5')");
		stmt.executeUpdate("INSERT INTO gross_requirements VALUES ('1', '2', '0')");
		stmt.executeUpdate("INSERT INTO gross_requirements VALUES ('1', '3', '0')");
		stmt.executeUpdate("INSERT INTO gross_requirements VALUES ('1', '4', '5')");
		stmt.executeUpdate("INSERT INTO gross_requirements VALUES ('2', '1', '0')");
		stmt.executeUpdate("INSERT INTO gross_requirements VALUES ('2', '2', '0')");
		stmt.executeUpdate("INSERT INTO gross_requirements VALUES ('2', '3', '0')");
		stmt.executeUpdate("INSERT INTO gross_requirements VALUES ('2', '4', '10')");

		rs = stmt.executeQuery("SELECT COUNT(component_id) AS numberOfComponents " + "FROM components");
		int numberOfComponents = rs.getInt("numberOfComponents");
		System.out.println(numberOfComponents + "\n");

		int periodNumber = 4;
		int part_id;
		int parent_id;
		int gross_requirement;
		int inventory_level;
		int net_requirement;
		int previousProjectedBalance;
		int lead_time;
		int order_release;
		int order_receipt;
		int projected_balance;

		for (int i = 0; i < numberOfComponents; i++) {

			rs = stmt.executeQuery("SELECT parent_id FROM components WHERE component_id = " + (i + 3) + "");
			parent_id = rs.getInt("parent_id");

			rs = stmt.executeQuery("SELECT inventory_level FROM components WHERE component_id = " + (i + 3) + "");
			inventory_level = rs.getInt("inventory_level");
			
			rs = stmt.executeQuery("SELECT lead_time FROM components WHERE component_id = " + (i + 3) + "");
			lead_time = rs.getInt("lead_time");

			for (int j = 0; j < periodNumber; j++) {

				stmt.executeUpdate("INSERT INTO MRP VALUES ('"+ (i+3) +"', '"+ (j+1) +"', '0', '0', '0', '0', '0')");

				// gross_requirement
				if (parent_id == 1 || parent_id == 2) {

					rs = stmt.executeQuery("SELECT gross_requirement FROM gross_requirements " + "WHERE part_id = "
							+ parent_id + " AND period = " + (j + 1) + "");
					gross_requirement = rs.getInt("gross_requirement");
					
					stmt.executeUpdate("UPDATE MRP SET gross_requirement = " + gross_requirement + "");
				} 
				else {

					rs = stmt.executeQuery("SELECT order_release FROM MRP " + "WHERE part_id = " + parent_id
							+ " AND period = " + (j + 1) + "");
					gross_requirement = rs.getInt("order_release");
					
					stmt.executeUpdate("UPDATE MRP SET gross_requirement = " + gross_requirement + "");
				}

				// net requirement
				if (j == 0) {

					net_requirement = gross_requirement - inventory_level;

					if (net_requirement <= 0) {

						net_requirement = 0;
					}
					
					stmt.executeUpdate("UPDATE MRP SET net_requirement = " + net_requirement + "");
				} 
				else {

					rs = stmt.executeQuery("SELECT projected_balance FROM MRP " + "WHERE period = " + j + "");
					previousProjectedBalance = rs.getInt("projected_balance");

					net_requirement = gross_requirement - previousProjectedBalance;
					if (net_requirement <= 0) {

						net_requirement = 0;
					}
					
					stmt.executeUpdate("UPDATE MRP SET net_requirement = " + net_requirement + "");

				}
				
				//order release
				rs = stmt.executeQuery("SELECT net_requirement FROM MRP WHERE period = " 
						+ ((j+1) + lead_time) + "");
				
				order_release = rs.getInt("net_requirement");
				
				stmt.executeUpdate("UPDATE MRP SET order_release = " + order_release + "");
				
				//order_receipt
				rs = stmt.executeQuery("SELECT order_release FROM MRP WHERE period = " 
						+ ((j+1) - lead_time) + "");
				
				order_receipt = rs.getInt("order_release");
				
				stmt.executeUpdate("UPDATE MRP SET order_receipt = " + order_receipt + "");

				//projected balance
				rs = stmt.executeQuery("SELECT projected_balance FROM MRP " + "WHERE period = " + j + "");
				previousProjectedBalance = rs.getInt("projected_balance");
				projected_balance = order_receipt + previousProjectedBalance - gross_requirement;
				
				stmt.executeUpdate("UPDATE MRP SET projected_balance = " + projected_balance + "");

				//insert to MRP
				//stmt.executeUpdate("INSERT INTO MRP VALUES ('" + (i + 3) + "', '" + (j + 1) + "', ' "+gross_requirement+"',"
						//+ "'"+projected_balance+"', '"+net_requirement+"', '"+order_release+"', '"+order_receipt+"')");
			}

		}
		
		//select order releases and part id s from MRP table and component names from components table and print
		//rs = stmt.executeQuery("SELECT part_id, order_release FROM MRP");
		rs = stmt.executeQuery("SELECT component_name, MRP.part_id, MRP.order_release "
				+ "FROM components INNER JOIN MRP ON MRP.part_id = components.component_id");
		while (rs.next()) {
			part_id = rs.getInt("part_id");
			order_release = rs.getInt("order_release");
			System.out.println("Part");
			System.out.println(part_id + " " + order_release + "\n");

		}

	}

}
