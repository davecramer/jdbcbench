/*
 *  This is a sample implementation of the Transaction Processing Performance
 *  Council Benchmark B coded in Java and ANSI SQL2.
 */

package rocks.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name="JDBCBench", version = "0.1")

public class JDBCBench implements Callable <Integer> {
	private enum Protocol  {
			SIMPLE,
			EXTENDED,
			PREPARED
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

	@Option(names={"--url, -u"}, description = "Url to connect to ")
	private  String DBUrl = "";

	@Option(names={"--transactions", "-t"}, description = "Number of transactions each client runs. Default is ${DEFAULT-VALUE}", defaultValue = "10")
	private int transactionsPerClient = 10;

	@Option(names={"--clients", "-c"}, description = "Number of clients simulated, that is, number of concurrent database sessions. Default is ${DEFAULT-VALUE}", defaultValue = "1")
	private int numClients = 10;

	@Option(names={"--select", "-s"}, description = "Run built-in select-only script.", defaultValue = "false")
	private boolean selectOnly = false;

	@Option(names = {"--vacuum-all", "-v"}, description = "Vacuum all four standard tables before running the test. With neither -n nor -v, pgbench will vacuum the pgbench_tellers and pgbench_branches tables, and will truncate pgbench_history.", defaultValue = "false")
	private boolean vacuumAll = false;

	@Option(names={"--no-vacuum", "-n"}, description = "Perform no vacuuming before running the test. This option is necessary if you are running a custom test scenario that does not include the standard tables", defaultValue = "false")
	private boolean noVacuum = false;

	@Option(names={"--protocol", "-M"}, description = "", defaultValue = "simple")
	private Protocol protocol= Protocol.SIMPLE;

	@Option(names={"--fillfactor", "-F"}, description = "Create the pgbench_accounts, pgbench_tellers and pgbench_branches tables with the given fill factor. Default is ${DEFAULT-VALUE}", defaultValue = "100")
	private int fillFactor = 100;

	@Option(names = {"--host", "-h"}, description = "Database server's host name", defaultValue = "localhost")
	private String host = "localhost";

	@Option(names={"--port", "-p"}, description = "Database server's port number", defaultValue = "5432")
	private int port = 5432;

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

	@Override
	public Integer call() {
		System.out
				.println("*********************************************************");
		System.out
				.println("* JDBCBench v1.0                                        *");
		System.out
				.println("*********************************************************");
		System.out.println();
		System.out.println("URL:" + DBUrl);
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
			Connection connection = DriverManager.getConnection(DBUrl);
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
				createDatabase(con);
				System.out.println("done.\n");
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
					clientCon = DriverManager.getConnection(DBUrl);
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

	/*
	 * createDatabase() - Creates and Initializes a scaled database.
	 */

	void createDatabase(Connection conn) {

		try (Statement stmt = conn.createStatement()) {

			String query = "CREATE TABLE branches (";
			query += "bid         INT NOT NULL, PRIMARY KEY(bid), ";
			query += "bbalance    INT,";
			query += "filler      CHAR(88))"; /* pad to 100 bytes */
			stmt.executeUpdate(query);
			stmt.clearWarnings();

			query = "CREATE TABLE tellers ( ";
			query += "tid         INT NOT NULL, PRIMARY KEY(tid),";
			query += "bid         INT,";
			query += "tbalance    INT,";
			query += "filler      CHAR(84))"; /* pad to 100 bytes */

			stmt.executeUpdate(query);
			stmt.clearWarnings();

			query = "CREATE TABLE accounts ( ";
			query += "aid         INT NOT NULL, PRIMARY KEY(aid), ";
			query += "bid         INT, ";
			query += "abalance    INT, ";
			query += "filler      CHAR(84))"; /* pad to 100 bytes */

			stmt.executeUpdate(query);
			stmt.clearWarnings();

			query = "CREATE TABLE history ( ";
			query += "tid         INT, ";
			query += "bid         INT, ";
			query += "aid         INT, ";
			query += "delta       INT, ";
			query += "time        TIMESTAMP, ";
			query += "filler      CHAR(22))"; /* pad to 50 bytes */

			stmt.executeUpdate(query);
			stmt.clearWarnings();

			/*
			 * prime database using TPC BM B scaling rules. Note that for each
			 * branch and teller: branch_id = teller_id / ntellers branch_id =
			 * account_id / naccounts
			 */

			for (int i = 0; i < numBranches * scale; i++) {
				query = "INSERT INTO branches(bid,bbalance) VALUES (" + i
						+ ",0)";
				stmt.executeUpdate(query);
				stmt.clearWarnings();
			}
			for (int i = 0; i < numTellers * scale; i++) {
				query = "INSERT INTO tellers(tid,bid,tbalance) VALUES (" + i
						+ "," + i / numTellers + ",0)";
				stmt.executeUpdate(query);
				stmt.clearWarnings();
			}
			for (int i = 0; i < numAccounts * scale; i++) {
				query = "INSERT INTO accounts(aid,bid,abalance) VALUES (" + i
						+ "," + i / numAccounts + ",0)";
				stmt.executeUpdate(query);
				stmt.clearWarnings();
			}
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			ex.printStackTrace();
		}

	} /* end of CreateDatabase */

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
					query += "FROM   accounts ";
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

				query = "UPDATE accounts ";
				query += "SET abalance = abalance + " + delta + " ";
				query += "WHERE aid = " + aid;

				stmt.executeUpdate(query);
				stmt.clearWarnings();

				query = "SELECT abalance ";
				query += "FROM   accounts ";
				query += "WHERE  aid = " + aid;

				try (ResultSet rs = stmt.executeQuery(query)) {
					stmt.clearWarnings();

					int aBalance = 0;

					while (rs.next()) {
						aBalance = rs.getInt(1);
					}

					query = "UPDATE tellers ";
					query += "SET    tbalance = tbalance + " + delta + " ";
					query += "WHERE  tid = " + tid;
					stmt.executeUpdate(query);
					stmt.clearWarnings();

					query = "UPDATE branches ";
					query += "SET    bbalance = bbalance + " + delta + " ";
					query += "WHERE  bid = " + bid;
					stmt.executeUpdate(query);
					stmt.clearWarnings();

					query = "INSERT INTO history(tid, bid, aid, delta) ";
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
