package rocks.postgres.util;

import java.util.ArrayList;
import java.util.List;

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

}
