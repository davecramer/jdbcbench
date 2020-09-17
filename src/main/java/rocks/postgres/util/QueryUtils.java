package rocks.postgres.util;

import java.sql.*;

public class QueryUtils {

    boolean useTransactions = false;
    boolean usePrepared = false;
    boolean useBinary = false;

    private final String selectQuery = "SELECT abalance FROM pgbench_accounts WHERE  aid = ?";
    // delta, aid
    private final String updateAccountsQuery = "update pgbench_accounts SET abalance = abalance + ? WHERE aid = ?" ;
    //delta, bid
    private final String updateBranchesQuery = "UPDATE pgbench_branches SET bbalance = bbalance + ? WHERE  bid = ?";
    // delta, tid
    private final String updateTellersQuery = "UPDATE pgbench_tellers SET tbalance = tbalance + ? WHERE  tid = ?";
    private final String insertHistoryQuery = "INSERT INTO pgbench_history(tid, bid, aid, delta) values (?,?,?,?)";

    public QueryUtils(boolean useTransactions, boolean usePrepared, boolean useBinary ) {
        this.useBinary = useBinary;
        this.usePrepared = usePrepared;
        this.useTransactions = useTransactions;
    }

    String prepareQuery(String query, int ...args ) {
        String replacedQuery = query;

        for (int i=0; i< args.length; i++){
            replacedQuery = replacedQuery.replaceFirst("\\?", Integer.toString(args[i]));
        }
        return replacedQuery;
    }

    public String prepareSelectQuery(int aid) {
        return prepareQuery(selectQuery, new int[] {aid} );
    }

    public String prepareUpdateTellersQuery(int delta, int tid) {
        return prepareQuery(updateTellersQuery, new int[] {delta, tid} );
    }

    public String prepareUpdateAccountsQuery(int delta, int aid) {
        return prepareQuery(updateAccountsQuery, new int[] {delta, aid} );
    }

    public String prepareUpdateBranchesQuery(int delta, int bid) {
        return prepareQuery(updateBranchesQuery, new int[] { delta, bid} );
    }
    public String prepareInsertHistoryQuery (int tid, int bid, int aid, int delta) {
        return prepareQuery(insertHistoryQuery, new int[] { tid, bid, aid, delta} );
    }

    PreparedStatement updateAccountsPstmt;
    public boolean executeUpdateAccounts(Connection con, int delta, int aid) throws SQLException {

        if ( usePrepared ) {
            if (updateAccountsPstmt == null ) {
                updateAccountsPstmt = con.prepareStatement(updateAccountsQuery);
            }
            updateAccountsPstmt.clearParameters();
            updateAccountsPstmt.setInt(1, delta);
            updateAccountsPstmt.setInt(2, aid);
            return updateAccountsPstmt.executeUpdate() == 1;
        } else {
            String query = prepareUpdateAccountsQuery(delta, aid);

            try (Statement stmt = con.createStatement()) {
                return stmt.executeUpdate(query) == 1;
            }
        }
    }

    PreparedStatement insertHistoryPstmt;
    public boolean executeInsertHistory(Connection con, int tid, int bid, int aid, int delta ) throws SQLException {

        if ( usePrepared ) {
            if ( insertHistoryPstmt == null ) {
                insertHistoryPstmt = con.prepareStatement(insertHistoryQuery);
            }
            insertHistoryPstmt.clearParameters();
            insertHistoryPstmt.setInt(1, tid );
            insertHistoryPstmt.setInt(2, bid );
            insertHistoryPstmt.setInt(3, aid );
            insertHistoryPstmt.setInt(4, delta );
            return insertHistoryPstmt.executeUpdate() == 1;


        } else {

            String query = prepareInsertHistoryQuery(tid, bid, aid, delta);

            try (Statement stmt = con.createStatement()) {
                return stmt.executeUpdate(query) == 1;
            }
        }
    }

    PreparedStatement updateBranchesPstmt;
    public boolean executeUpdateBranchesQuery(Connection con, int delta, int bid ) throws SQLException {

        if ( usePrepared ) {
            if (updateBranchesPstmt == null ) {
                updateBranchesPstmt = con.prepareStatement(updateBranchesQuery);
            }
            updateBranchesPstmt.clearParameters();
            updateBranchesPstmt.setInt(1, delta);
            updateBranchesPstmt.setInt(2, bid);
            return updateBranchesPstmt.executeUpdate() == 1;

        } else {
            String query = prepareUpdateBranchesQuery(delta, bid);

            try (Statement stmt = con.createStatement()) {
                return stmt.executeUpdate(query) == 1;
            }
        }
    }
    PreparedStatement updateTellersPstmt;
    public boolean executeUpdateTellersQuery(Connection con, int delta, int tid) throws SQLException {
        if ( usePrepared ) {
            if ( updateTellersPstmt == null ) {
                updateTellersPstmt = con.prepareStatement(updateTellersQuery);
            }
            updateTellersPstmt.clearParameters();
            updateTellersPstmt.setInt(1, delta);
            updateTellersPstmt.setInt(2, tid);
            return updateTellersPstmt.executeUpdate() == 1;
        } else {
            String query = prepareUpdateTellersQuery(delta, tid);

            try (Statement stmt = con.createStatement()) {
                return stmt.executeUpdate(query) == 1;
            }
        }
    }
    PreparedStatement selectPstmt;
    public int executeSelectQuery(Connection con, int aid) throws SQLException {
        if ( usePrepared ) {
            if (selectPstmt == null ) {
                selectPstmt = con.prepareStatement( selectQuery );
            }
            selectPstmt.clearParameters();
            selectPstmt.setInt(1, aid);
            try (ResultSet rs = selectPstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                } else {
                    return -1;
                }

            }
        }
        String query = prepareSelectQuery(aid);
        try ( Statement stmt = con.createStatement() ) {
            try (ResultSet rs = stmt.executeQuery(query)) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                else {
                    return -1;
                }
            }
        }
    }
}
