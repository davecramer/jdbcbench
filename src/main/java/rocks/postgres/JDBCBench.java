/*
 *  This is a sample implementation of the Transaction Processing Performance
 *  Council Benchmark B coded in Java and ANSI SQL2.
 */

package rocks.postgres;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.swing.plaf.nimbus.State;

@Command(name="JDBCBench", version = "0.1")

public class JDBCBench implements Callable <Integer> {
	private enum Protocol  {
			SIMPLE("simple"),
			EXTENDED("extended"),
			PREPARED("prepared");

		Protocol(String label){
				this.label = label;
				this.name = label;
		}
		private final String label;
		private final String name;
		public String getName() {
			return name;
		}
	}

	private enum InitSteps {
		DROP,
		CREATE_TABLES,
		VACUUM,
		PRIMARY_KEYS,
		FOREIGN_KEYS
	}

	/* tpc bm b scaling rules */
	@Option(names = {"-s, --scale"}, description = "scale factor")
	private static int scale = 1; /* the database scaling factor */
	private static int numBranches = 1; /* number of branches in 1 scale db */
	private static int numTellers = 10; /* number of tellers in 1 scale db */
	private static int numAccounts = 10000; /* number of accounts in 1 scale db */
	private static int numHistory = 864000; /* number of history recs in 1 scale db */

	public final static int TELLER = 0;
	public final static int BRANCH = 1;
	public final static int ACCOUNT = 2;

	/* we will use host, port, user, database */
	@Deprecated

	@Option(names={"--clients", "-c"}, description = "Number of clients simulated, that is, number of concurrent database sessions. Default is ${DEFAULT-VALUE}", defaultValue = "1")
	private int numClients = 10;

	@Option(names={"--fillfactor", "-F"}, description = "Create the pgbench_accounts, pgbench_tellers and pgbench_branches tables with the given fill factor. Default is ${DEFAULT-VALUE}", defaultValue = "100")
	private int fillFactor = 100;

	@Option(names = {"--drop-tables"}, description = "Drop tables before create", defaultValue = "false")
	private boolean dropTables = false;

	@Option(names = {"--big-integers"}, description = "Use bigInteger for account id .", defaultValue = "false")
	private boolean bigIntegers = false;

	@Option(names = {"--primary-keys"}, description = "Create primary key indexes on  the standard tables.", defaultValue = "false")
	private boolean primaryKeys = false;

	@Option(names = {"--foreign-keys"}, description = "Create foreign key constraints between the standard tables.", defaultValue = "false")
	private boolean foreignKeys = false;

	@Option(names = {"--truncate"}, description = "truncate tables before run", defaultValue = "false")
	private boolean truncate = false;

	@Option(names = {"--host", "-h"}, description = "Database server's host name", defaultValue = "localhost")
	private String host = "localhost";

	@Option(names={"--no-vacuum", "-n"}, description = "Perform no vacuuming before running the test. This option is necessary if you are running a custom test scenario that does not include the standard tables", defaultValue = "false")
	private boolean noVacuum = false;

	@Option(names={"--port", "-p"}, description = "Database server's port number", defaultValue = "5432")
	private int port = 5432;

	@Option(names={"--protocol", "-M"}, description = "", defaultValue = "SIMPLE")
	private Protocol protocol= Protocol.SIMPLE;

	@Option(names={"--select", "-s"}, description = "Run built-in select-only script.", defaultValue = "false")
	private boolean selectOnly = false;

	@Option(names={"--transactions", "-t"}, description = "Number of transactions each client runs. Default is ${DEFAULT-VALUE}", defaultValue = "10")
	private int transactionsPerClient = 10;

	@Option(names = {"--vacuum-all", "-v"}, description = "Vacuum all four standard tables before running the test. With neither -n nor -v, pgbench will vacuum the pgbench_tellers and pgbench_branches tables, and will truncate pgbench_history.", defaultValue = "false")
	private boolean vacuumAll = false;

	@Parameters(defaultValue = "pgbench", description = "Database Name")
	private String dbName;

	@Option(names={"--username", "-u"}, description = "User name to authenticate as", defaultValue = "current user")
	private String user = System.getProperty("user.name");

	@Option(names={"--initialize", "-i"}, description = "Initializes database, tables and data", defaultValue = "false")
	private boolean initializeDataset=false;

	@Option(names={"--verbose"}, description = "Verbose output", defaultValue = "false")
	private boolean verbose = false;

	private boolean isTransactionBlock = true;

	private int failedTransactions = 0;
	private int transactionCount = 0;
	private long startTime = 0;


	private MemoryWatcherThread MemoryWatcher;

	private final String jdbcProtocol = "jdbc:postgresql://";

	private String createUrl(String host, int port, String database){
		StringBuilder stringBuilder = new StringBuilder(jdbcProtocol)
				.append(host)
				.append(':')
				.append(port)
				.append('/')
				.append(database);
		return stringBuilder.toString();
	}
	String dbUrl;

	@Override
	public Integer call() {
		dbUrl = createUrl(host, port, dbName);
		System.out
				.println("*********************************************************");
		System.out
				.println("* JDBCBench v1.0                                        *");
		System.out
				.println("*********************************************************");
		System.out.println();
		System.out.println("URL:" + dbUrl );
		System.out.println();
		System.out.println("Number of clients: " + numClients);
		System.out.println("Number of transactions per client: "
				+ transactionsPerClient);

		if (selectOnly) {
			System.out.println("Transaction mode:  SELECT-only");
		} else {
			System.out.println("Transaction mode:  TPC-B like");
		}

		System.out.println();

		try {
			Connection connection = DriverManager.getConnection(dbUrl);
			executeTest(connection, initializeDataset);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		return 0;
	}
	/*
	 * main program, creates a 1-scale database: i.e. 1 branch, 10 tellers,...
	 * runs one TPC BM B transaction
	 */

	public static void main(String[] args) {
		new CommandLine(new JDBCBench()).execute(args);

	}

	public JDBCBench() {
	}

	public void executeTest(Connection con, boolean init) {
		int clientCount;
		List<Thread> clients = new ArrayList<Thread>();
		Thread t;

		try {
			if (init) {
				System.out.print("Initializing dataset...");
				createDatabase(con, primaryKeys, foreignKeys, bigIntegers );
				System.out.println("done.\n");
			}

			if ( vacuumAll ) {
				System.out.println("Vacuuming tables ");
				vacuum( con );
			}

			System.out.println("* Starting Benchmark Run *");
			MemoryWatcher = new MemoryWatcherThread();
			MemoryWatcher.start();

			startTime = System.nanoTime();

			/*
			 * Cache the client count because once threads start, if the
			 * transaction count is low they can finish and alter n_clients
			 * before all the client threads have even been started.
			 */
			clientCount = numClients;

			for (int i = 0; i < numClients; i++) {
				Connection clientCon;
				/* Re-use the existing connection for the first client */
				if (i == 0) {
					clientCon = con;
				} else {
					clientCon = DriverManager.getConnection(dbUrl);
				}

				Thread clientThread = new ClientThread(transactionsPerClient, i,
						clientCon);
				clients.add(clientThread);
			}

			/*
			 * Start clients only after all have been created to avoid a race
			 * condition. Fast ending clients were reporting that all clients
			 * were finished before some had even been started.
			 */
			for (int i = 0; i < clientCount; i++) {
				System.out.println("Starting client " + (i + 1));
				t = clients.get(i);
				t.start();
			}

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void reportDone() {
		numClients--;

		if (numClients <= 0) {
			MemoryWatcher.interrupt();
			long endTime = System.nanoTime();
			double completion_time = ((double) endTime - (double) startTime) / 1E9;
			System.out.println("* Benchmark finished *");
			System.out.println("\n* Benchmark Report *");
			System.out.println("--------------------\n");
			System.out.println("Time to execute " + transactionCount
					+ " transactions: " + completion_time + " seconds.");
			System.out.println("Max/Min memory usage: " + MemoryWatcher.max
					+ " / " + MemoryWatcher.min + " kb");
			System.out.println(failedTransactions + " / " + transactionCount
					+ " failed to complete.");
			System.out.println("Transaction rate: "
					+ (transactionCount - failedTransactions)
					/ completion_time + " txn/sec.");
		}

	}

	public synchronized void incrementTransactionCount() {
		transactionCount++;
	}

	public synchronized void incrementFailedTransactionCount() {
		failedTransactions++;
	}

	void runInitSteps(List <InitSteps> initSteps ) throws SQLException {

	}
	/*
	 * createDatabase() - Creates and Initializes a scaled database.
	 */

	void dropTables( Connection con ) throws  SQLException {
		try (Statement stmt = con.createStatement()) {
			stmt.execute("drop table if exists "
					+ "pgbench_accounts, "
					+ "pgbench_branches, "
					+ "pgbench_history, "
					+ "pgbench_tellers");
		}
	}
	void initTruncateTables(Connection con) throws SQLException
	{
		try ( Statement stmt = con.createStatement() ) {
			stmt.execute("truncate table "
					+ "pgbench_accounts, "
					+ "pgbench_branches, "
					+ "pgbench_history, "
					+ "pgbench_tellers");
		}
	}
	void initCreatePKeys(Connection con) throws SQLException
	{
		final String[] DDL_INDEXES = {
				"alter table pgbench_branches add primary key (bid)",
				"alter table pgbench_tellers add primary key (tid)",
				"alter table pgbench_accounts add primary key (aid)"
		};

		try ( Statement stmt = con.createStatement() ) {
			for ( int i=0; i< DDL_INDEXES.length; i++ ) {
				stmt.execute(DDL_INDEXES[i]);
			}
		}
	}

	void initCreateFKeys(Connection con) throws SQLException
	{
		final String[] DDL_KEYS = {
				"alter table pgbench_tellers add constraint pgbench_tellers_bid_fkey foreign key (bid) references pgbench_branches",
				"alter table pgbench_accounts add constraint pgbench_accounts_bid_fkey foreign key (bid) references pgbench_branches",
				"alter table pgbench_history add constraint pgbench_history_bid_fkey foreign key (bid) references pgbench_branches",
				"alter table pgbench_history add constraint pgbench_history_tid_fkey foreign key (tid) references pgbench_tellers",
				"alter table pgbench_history add constraint pgbench_history_aid_fkey foreign key (aid) references pgbench_accounts"		};

		try ( Statement stmt = con.createStatement() ) {
			for ( int i=0; i< DDL_KEYS.length; i++ ) {
				stmt.execute(DDL_KEYS[i]);
			}
		}
	}
	private class PgColumn {
		String name;
		String type;
		PgColumn (String name, String type) {
			this.name = name;
			this.type = type;
		}
	}
	private class DDLInfo {
		String tableName;
		List <PgColumn> columnList;
		DDLInfo( String tableName ) {
			this.tableName = tableName;
			columnList = new ArrayList<>();
		}
		DDLInfo addColumn(PgColumn pgColumn) {
			columnList.add(pgColumn);
			return this;
		}
		String createTable() {
			StringBuilder sb = new StringBuilder("create table ")
					.append(tableName)
					.append('(');
			for(int i=0; i< columnList.size();i++) {
				PgColumn column = columnList.get(i);
				sb.append(' ')
				.append(column.name)
				.append(' ')
				.append(column.type);
				if ( (i+1) < columnList.size() ) {
					sb.append(',');
				}

			}
			sb.append(')');
			return sb.toString();
		}
	}

	void createDatabase(Connection conn, boolean foreignKeys, boolean primaryKeys, boolean bigIntegers) {

		String query;
		final List <DDLInfo>  tableList = new ArrayList<DDLInfo>();
		tableList.add( new DDLInfo("pgbench_history").addColumn(new PgColumn("tid", "int"))
				.addColumn(new PgColumn("bid", "int"))
				.addColumn(new PgColumn("aid", bigIntegers?"bigint":"int"))
				.addColumn(new PgColumn("delta", "int"))
				.addColumn(new PgColumn("mtime", "timestamp"))
				.addColumn(new PgColumn("filler", "char(22)")));

		tableList.add( new DDLInfo( "pgbench_tellers")
				.addColumn(new PgColumn("tid", "int not null"))
				.addColumn(new PgColumn("bid", "int"))
				.addColumn(new PgColumn("tbalance", "int"))
				.addColumn(new PgColumn("filler", "char(84)")));

		tableList.add( new DDLInfo( "pgbench_accounts")
				.addColumn(new PgColumn("aid", bigIntegers?"bigint":"int" + " not null"))
				.addColumn(new PgColumn("bid", "int"))
				.addColumn(new PgColumn("abalance", "int"))
				.addColumn(new PgColumn("filler", "char(84)")));

		tableList.add( new DDLInfo( "pgbench_branches")
				.addColumn(new PgColumn("bid", "int not null"))
				.addColumn(new PgColumn("bbalance", "int"))
				.addColumn(new PgColumn("filler", "char(88)")));

		try {
			if (dropTables) {
				dropTables(conn);
			}
		} catch (Exception ex ) {
			System.err.println("Error dropping tables ");
			ex.printStackTrace(System.err);
		}

		try (Statement stmt = conn.createStatement()) {

			for ( DDLInfo tableDDL:tableList ) {
				String q = tableDDL.createTable();
				stmt.executeUpdate(q);
				stmt.clearWarnings();
			}

			if (primaryKeys) {
				initCreatePKeys(conn);
			}

			if (foreignKeys) {
				initCreateFKeys(conn);
			}

			/*
			 * prime database using TPC BM B scaling rules. Note that for each
			 * branch and teller: branch_id = teller_id / ntellers branch_id =
			 * account_id / naccounts
			 */

			for (int i = 0; i < numBranches * scale; i++) {
				query = "INSERT INTO pgbench_branches(bid,bbalance) VALUES (" + i
						+ ",0)";
				stmt.executeUpdate(query);
				stmt.clearWarnings();
			}
			for (int i = 0; i < numTellers * scale; i++) {
				query = "INSERT INTO pgbench_tellers(tid,bid,tbalance) VALUES (" + i
						+ "," + i / numTellers + ",0)";
				stmt.executeUpdate(query);
				stmt.clearWarnings();
			}
			for (int i = 0; i < numAccounts * scale; i++) {
				query = "INSERT INTO pgbench_accounts(aid,bid,abalance) VALUES (" + i
						+ "," + i / numAccounts + ",0)";
				stmt.executeUpdate(query);
				stmt.clearWarnings();
			}
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			ex.printStackTrace();
		}

	} /* end of CreateDatabase */

	private void vacuum( Connection con ) throws SQLException {
		try (Statement statement = con.createStatement() ) {
			statement.executeQuery("vacuum");
		}
	}

	public static int getRandomInt(int lo, int hi) {
		int ret;

		ret = (int) (Math.random() * (hi - lo + 1));
		ret += lo;

		return ret;
	}

	public static int getRandomID(int type) {
		int min, max, num;

		min = 0;
		num = numAccounts;
		if (type == TELLER)
			num = numTellers;
		else if (type == BRANCH)
			num = numBranches;
		else if (type == ACCOUNT)
			num= numAccounts;
		max = min + num - 1;
		return (getRandomInt(min, max));
	}

	class ClientThread extends Thread {
		int ntrans;
		int clientid;
		Connection connection;

		public ClientThread(int number_of_txns, int id, Connection C) {
			ntrans = number_of_txns;
			clientid = id;
			connection = C;
		}

		public void run() {
			while (ntrans-- > 0) {
				int account = JDBCBench.getRandomID(ACCOUNT);
				int branch = JDBCBench.getRandomID(BRANCH);
				int teller = JDBCBench.getRandomID(TELLER);
				int delta = JDBCBench.getRandomInt(-500, 500);
				doOne(account, branch, teller, delta);
				incrementTransactionCount();
			}
			reportDone();
		}

		/*
		 * doOne() - Executes a single TPC BM B transaction.
		 */
		int doOne(int aid, int bid, int tid, int delta) {
			String query;

			try (Statement stmt = connection.createStatement()) {

				if (selectOnly) {
					query = "SELECT abalance ";
					query += "FROM   pgbench_accounts ";
					query += "WHERE  aid = " + aid;

					try (ResultSet rs = stmt.executeQuery(query)) {
						stmt.clearWarnings();

						while (rs.next()) {
							rs.getInt(1);
						}
						return 0;
					}
				}

				if (isTransactionBlock) {
					connection.setAutoCommit(false);
				}

				query = "UPDATE pgbench_accounts ";
				query += "SET abalance = abalance + " + delta + " ";
				query += "WHERE aid = " + aid;

				stmt.executeUpdate(query);
				stmt.clearWarnings();

				query = "SELECT abalance ";
				query += "FROM   pgbench_accounts ";
				query += "WHERE  aid = " + aid;

				try (ResultSet rs = stmt.executeQuery(query)) {
					stmt.clearWarnings();

					int aBalance = 0;

					while (rs.next()) {
						aBalance = rs.getInt(1);
					}

					query = "UPDATE pgbench_tellers ";
					query += "SET    tbalance = tbalance + " + delta + " ";
					query += "WHERE  tid = " + tid;
					stmt.executeUpdate(query);
					stmt.clearWarnings();

					query = "UPDATE pgbench_branches ";
					query += "SET    bbalance = bbalance + " + delta + " ";
					query += "WHERE  bid = " + bid;
					stmt.executeUpdate(query);
					stmt.clearWarnings();

					query = "INSERT INTO pgbench_history(tid, bid, aid, delta) ";
					query += "VALUES (";
					query += tid + ",";
					query += bid + ",";
					query += aid + ",";
					query += delta + ")";
					stmt.executeUpdate(query);
					stmt.clearWarnings();

					if (isTransactionBlock) {
						connection.commit();
					}
					return aBalance;

				} catch (SQLException e) {
					if (verbose) {
						System.err.println("Transaction failed: " + e.getMessage());
						e.printStackTrace();
					}
					incrementFailedTransactionCount();
				}
				return 0;

			} catch (SQLException ex) {
				ex.printStackTrace(); /* end of DoOne */
				return 0;
			}
		}
	}

	static class MemoryWatcherThread extends Thread {
		long min = 0;
		long max = 0;
		boolean running;

		public void run() {
			running = true;
			min = Runtime.getRuntime().freeMemory();

			while (running) {
				long currentFree = Runtime.getRuntime().freeMemory();
				long currentAlloc = Runtime.getRuntime().totalMemory();
				long used = currentAlloc - currentFree;

				if (used < min)
					min = used;
				if (used > max)
					max = used;

				try {
					sleep(100);
				} catch (InterruptedException e) {
					running = false;
				}
			}
		}
	}

}
