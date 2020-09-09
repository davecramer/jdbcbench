/* 
 *  This is a sample implementation of the Transaction Processing Performance 
 *  Council Benchmark B coded in Java and ANSI SQL2. 
 */

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCBench {

	/* tpc bm b scaling rules */
	public static int scale = 1; /* the database scaling factor */
	public static int nbranches = 1; /* number of branches in 1 scale db */
	public static int ntellers = 10; /* number of tellers in 1 scale db */
	public static int naccounts = 10000; /* number of accounts in 1 scale db */
	public static int nhistory = 864000; /* number of history recs in 1 scale db */

	public final static int TELLER = 0;
	public final static int BRANCH = 1;
	public final static int ACCOUNT = 2;

	public static String DBUrl = "";
	public static String DriverName = "";
	public static boolean trans_block = true;
	public static boolean select_only = false;
	
	int failed_transactions = 0;
	int transaction_count = 0;
	static int n_clients = 10;
	static int n_txn_per_client = 10;
	long start_time = 0;

	static boolean verbose = false;

	MemoryWatcherThread MemoryWatcher;

	/*
	 * main program, creates a 1-scale database: i.e. 1 branch, 10 tellers,...
	 * runs one TPC BM B transaction
	 */

	public static void main(String[] Args) {
		boolean initialize_dataset = false;

		for (int i = 0; i < Args.length; i++) {
			if (Args[i].equals("-clients")) {
				if (i + 1 < Args.length) {
					i++;
					n_clients = Integer.parseInt(Args[i]);
				}
			} else if (Args[i].equals("-driver")) {
				if (i + 1 < Args.length) {
					i++;
					DriverName = Args[i];
				}
			} else if (Args[i].equals("-url")) {
				if (i + 1 < Args.length) {
					i++;
					DBUrl = Args[i];
				}
			} else if (Args[i].equals("-tpc")) {
				if (i + 1 < Args.length) {
					i++;
					n_txn_per_client = Integer.parseInt(Args[i]);
				}
			} else if (Args[i].equals("-init")) {
				initialize_dataset = true;
			} else if (Args[i].equals("-notrans")) {
				trans_block = false;
			} else if (Args[i].equals("-S")) {
				select_only = true;
			} else if (Args[i].equals("-v")) {
				verbose = true;
			}
		}

		if (DriverName.length() == 0 || DBUrl.length() == 0) {
			System.out
					.println("usage: java JDBCBench -driver [driver_class_name] -url [url_to_db] [-v] [-init] [-notrans] [-tpc n] [-clients]");
			System.out.println();
			System.out.println("-v 		verbose error messages");
			System.out.println("-init 	initialize the tables");
			System.out
					.println("-notrans use auto-commit, not transaction mode");
			System.out.println("-S	SELECT-only read mode");
			System.out.println("-tpc	transactions per client");
			System.out.println("-clients    number of simultaneous clients");
			System.exit(-1);
		}

		System.out
				.println("*********************************************************");
		System.out
				.println("* JDBCBench v1.0                                        *");
		System.out
				.println("*********************************************************");
		System.out.println();
		System.out.println("Driver: " + DriverName);
		System.out.println("URL:" + DBUrl);
		System.out.println();
		System.out.println("Number of clients: " + n_clients);
		System.out.println("Number of transactions per client: "
				+ n_txn_per_client);

		if (select_only)
			System.out.println("Transaction mode:  SELECT-only");
		else
			System.out.println("Transaction mode:  TPC-B like");

		System.out.println();

		try {
			Class.forName(DriverName);
			Connection C = DriverManager.getConnection(DBUrl);
			new JDBCBench(C, initialize_dataset);
		} catch (Exception E) {
			System.out.println(E.getMessage());
			E.printStackTrace();
		}
	}

	public JDBCBench(Connection C, boolean init) {
		int client_count;
		List clients = new ArrayList();
		Thread t;

		try {
			if (init) {
				System.out.print("Initializing dataset...");
				createDatabase(C);
				System.out.println("done.\n");
			}
			System.out.println("* Starting Benchmark Run *");
			MemoryWatcher = new MemoryWatcherThread();
			MemoryWatcher.start();

			start_time = System.currentTimeMillis();

			/*
			 * Cache the client count because once threads start, if the
			 * transaction count is low they can finish and alter n_clients
			 * before all the client threads have even been started.
			 */
			client_count = n_clients;

			for (int i = 0; i < n_clients; i++) {
				Connection client_con;
				/* Re-use the existing connection for the first client */
				if (i == 0)
					client_con = C;
				else
					client_con = DriverManager.getConnection(DBUrl);

				Thread Client = new ClientThread(n_txn_per_client, i,
						client_con);
				clients.add(Client);
			}

			/*
			 * Start clients only after all have been created to avoid a race
			 * condition. Fast ending clients were reporting that all clients
			 * were finished before some had even been started.
			 */
			for (int i = 0; i < client_count; i++) {
				System.out.println("Starting client " + (i + 1));
				t = (Thread) clients.get(i);
				t.start();
			}

		} catch (Exception E) {
			System.out.println(E.getMessage());
			E.printStackTrace();
		}
	}

	public void reportDone() {
		n_clients--;

		if (n_clients <= 0) {
			MemoryWatcher.interrupt();
			long end_time = System.currentTimeMillis();
			double completion_time = ((double) end_time - (double) start_time) / 1000;
			System.out.println("* Benchmark finished *");
			System.out.println("\n* Benchmark Report *");
			System.out.println("--------------------\n");
			System.out.println("Time to execute " + transaction_count
					+ " transactions: " + completion_time + " seconds.");
			System.out.println("Max/Min memory usage: " + MemoryWatcher.max
					+ " / " + MemoryWatcher.min + " kb");
			System.out.println(failed_transactions + " / " + transaction_count
					+ " failed to complete.");
			System.out.println("Transaction rate: "
					+ (transaction_count - failed_transactions)
					/ completion_time + " txn/sec.");
		}

	}

	public synchronized void incrementTransactionCount() {
		transaction_count++;
	}

	public synchronized void incrementFailedTransactionCount() {
		failed_transactions++;
	}

	/*
	 * createDatabase() - Creates and Initializes a scaled database.
	 */

	void createDatabase(Connection Conn) throws Exception {

		try {
			Statement Stmt = Conn.createStatement();

			String Query = "CREATE TABLE branches (";
			Query += "Bid         INT NOT NULL, PRIMARY KEY(Bid), ";
			Query += "Bbalance    INT,";
			Query += "filler      CHAR(88))"; /* pad to 100 bytes */
			Stmt.executeUpdate(Query);
			Stmt.clearWarnings();

			Query = "CREATE TABLE tellers ( ";
			Query += "Tid         INT NOT NULL, PRIMARY KEY(Tid),";
			Query += "Bid         INT,";
			Query += "Tbalance    INT,";
			Query += "filler      CHAR(84))"; /* pad to 100 bytes */

			Stmt.executeUpdate(Query);
			Stmt.clearWarnings();

			Query = "CREATE TABLE accounts ( ";
			Query += "Aid         INT NOT NULL, PRIMARY KEY(Aid), ";
			Query += "Bid         INT, ";
			Query += "Abalance    INT, ";
			Query += "filler      CHAR(84))"; /* pad to 100 bytes */

			Stmt.executeUpdate(Query);
			Stmt.clearWarnings();

			Query = "CREATE TABLE history ( ";
			Query += "Tid         INT, ";
			Query += "Bid         INT, ";
			Query += "Aid         INT, ";
			Query += "delta       INT, ";
			Query += "time        TIMESTAMP, ";
			Query += "filler      CHAR(22))"; /* pad to 50 bytes */

			Stmt.executeUpdate(Query);
			Stmt.clearWarnings();

			/*
			 * prime database using TPC BM B scaling rules. Note that for each
			 * branch and teller: branch_id = teller_id / ntellers branch_id =
			 * account_id / naccounts
			 */

			for (int i = 0; i < nbranches * scale; i++) {
				Query = "INSERT INTO branches(Bid,Bbalance) VALUES (" + i
						+ ",0)";
				Stmt.executeUpdate(Query);
				Stmt.clearWarnings();
			}
			for (int i = 0; i < ntellers * scale; i++) {
				Query = "INSERT INTO tellers(Tid,Bid,Tbalance) VALUES (" + i
						+ "," + i / ntellers + ",0)";
				Stmt.executeUpdate(Query);
				Stmt.clearWarnings();
			}
			for (int i = 0; i < naccounts * scale; i++) {
				Query = "INSERT INTO accounts(Aid,Bid,Abalance) VALUES (" + i
						+ "," + i / naccounts + ",0)";
				Stmt.executeUpdate(Query);
				Stmt.clearWarnings();
			}
		} catch (Exception E) {
			System.out.println(E.getMessage());
			E.printStackTrace();
		}

	} /* end of CreateDatabase */

	public static int getRandomInt(int lo, int hi) {
		int ret = 0;

		ret = (int) (Math.random() * (hi - lo + 1));
		ret += lo;

		return ret;
	}

	public static int getRandomID(int type) {
		int min, max, num;

		max = min = 0;
		num = naccounts;
		if (type == TELLER)
			num = ntellers;
		else if (type == BRANCH)
			num = nbranches;
		else if (type == ACCOUNT)
			num=naccounts;
		max = min + num - 1;
		return (getRandomInt(min, max));
	}

	class ClientThread extends Thread {
		int ntrans = 0;
		int clientid;
		Connection Conn;

		public ClientThread(int number_of_txns, int id, Connection C) {
			ntrans = number_of_txns;
			clientid = id;
			Conn = C;
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
			String Query;

			try {
				Statement Stmt = Conn.createStatement();

				if (select_only)
				{
					Query = "SELECT Abalance ";
					Query += "FROM   accounts ";
					Query += "WHERE  Aid = " + aid;

					ResultSet RS = Stmt.executeQuery(Query);
					Stmt.clearWarnings();

					int aBalance = 0;

					while (RS.next()) {
						aBalance = RS.getInt(1);
					}					
					return 0;
				}
				
				if (trans_block)
					Conn.setAutoCommit(false);

				Query = "UPDATE accounts ";
				Query += "SET     Abalance = Abalance + " + delta + " ";
				Query += "WHERE   Aid = " + aid;

				Stmt.executeUpdate(Query);
				Stmt.clearWarnings();

				Query = "SELECT Abalance ";
				Query += "FROM   accounts ";
				Query += "WHERE  Aid = " + aid;

				ResultSet RS = Stmt.executeQuery(Query);
				Stmt.clearWarnings();

				int aBalance = 0;

				while (RS.next()) {
					aBalance = RS.getInt(1);
				}

				Query = "UPDATE tellers ";
				Query += "SET    Tbalance = Tbalance + " + delta + " ";
				Query += "WHERE  Tid = " + tid;
				Stmt.executeUpdate(Query);
				Stmt.clearWarnings();

				Query = "UPDATE branches ";
				Query += "SET    Bbalance = Bbalance + " + delta + " ";
				Query += "WHERE  Bid = " + bid;
				Stmt.executeUpdate(Query);
				Stmt.clearWarnings();

				Query = "INSERT INTO history(Tid, Bid, Aid, delta) ";
				Query += "VALUES (";
				Query += tid + ",";
				Query += bid + ",";
				Query += aid + ",";
				Query += delta + ")";
				Stmt.executeUpdate(Query);
				Stmt.clearWarnings();

				if (trans_block)
					Conn.commit();

				return aBalance;
			} catch (SQLException E) {
				if (verbose) {
					System.out.println("Transaction failed: " + E.getMessage());
					E.printStackTrace();
				}
				incrementFailedTransactionCount();
			}
			return 0;

		} /* end of DoOne */

	}

	class MemoryWatcherThread extends Thread {
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
				} catch (InterruptedException E) {
					running = false;
				}
			}
		}
	}

}
