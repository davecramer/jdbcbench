package rocks.postgres.util;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class QueryUtilsTest {
    QueryUtils queryUtils;

    @Before
    public void setup() {
        queryUtils = new QueryUtils(false, false, false );
    }

    @Test
    public void testPrepareSelectQuery() {
        String preparedQuery = queryUtils.prepareSelectQuery(4);
        Assert.assertEquals("SELECT abalance FROM pgbench_accounts WHERE  aid = 4", preparedQuery);
    }
    @Test
    public void testPrepareUpdateTellersQuery() {
        String preparedQuery = queryUtils.prepareUpdateTellersQuery(1001, 10);
        Assert.assertEquals("UPDATE pgbench_tellers SET tbalance = tbalance + 1001 WHERE  tid = 10", preparedQuery);
    }

    @Test
    public void testPrepareUpdateAccountsQuery() {
        String preparedQuery = queryUtils.prepareUpdateAccountsQuery(1111, 55);
        Assert.assertEquals("update pgbench_accounts SET abalance = abalance + 1111 WHERE aid = 55", preparedQuery);
    }

    @Test
    public void testPrepareUpdateBranchesQuery() {
        String preparedQuery = queryUtils.prepareUpdateBranchesQuery(1234, 99);
        Assert.assertEquals("UPDATE pgbench_branches SET bbalance = bbalance + 1234 WHERE  bid = 99", preparedQuery);
    }
    @Test
    public void testPrepareInsertQuery() {
        String preparedQuery = queryUtils.prepareInsertHistoryQuery(1,2,3,4);
        Assert.assertEquals("INSERT INTO pgbench_history(tid, bid, aid, delta) values (1,2,3,4)", preparedQuery);
    }

}