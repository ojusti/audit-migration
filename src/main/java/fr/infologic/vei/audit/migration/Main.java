package fr.infologic.vei.audit.migration;

import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import oracle.jdbc.pool.OracleDataSource;


public class Main
{
    private static int numberOfThreads;
    private static String oracleDBURL;
    private static String dBUser;
    private static String oracleDBPassword;
    private static AuditOracleDataSource oracle;

    public static void main(String[] args) throws SQLException
    {
        parseArgs(args);
        oracleDataSource().forEachKey(migrateToMongo());
    }
    
    private static void parseArgs(String[] args)
    {
        numberOfThreads = Integer.parseInt(args[0]);
        oracleDBURL = args[1];
        dBUser = args[2];
        oracleDBPassword = args.length == 3 ? args[2] : args[3];
    }
    
    private static AuditOracleDataSource oracleDataSource() throws SQLException
    {
        OracleDataSource db = new OracleDataSource();
        db.setURL(oracleDBURL);
        db.setUser(dBUser);
        db.setPassword(oracleDBPassword);
        return oracle = new AuditOracleDataSource(db);
    }
    
    private static AuditMongoDataSink mongo()
    {
        return new AuditMongoDataSink(dBUser);
    }
    
    private static ThreadPoolExecutor executor()
    {
        return new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(500), new CallerRunsPolicy());
    }
    
    private static Migration migrateToMongo()
    {
        return new Migration(oracle, executor(), mongo());
    }
}
