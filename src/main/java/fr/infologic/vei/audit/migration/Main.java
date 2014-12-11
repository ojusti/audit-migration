package fr.infologic.vei.audit.migration;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import oracle.jdbc.pool.OracleDataSource;


public class Main
{
    private static int numberOfThreads;
    private static String oracleDBURL;
    private static String dBUser;
    private static String oracleDBPassword;
    private static AuditOracleDataSource oracle;
    private static AuditMongoDataSink mongo;
    private static ThreadPoolExecutor executor;

    public static void main(String... args) throws SQLException, InterruptedException, IOException
    {
        parseArgs(args);
        oracleDataSource().forEachKey(migrateToMongo());
        checkNumberOfDocuments();
        
    }

    private static void checkNumberOfDocuments() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
        System.out.println(oracle.mesure);
        System.out.println(mongo.mesure);
        int recordsInOracle = oracle.count();
        int recordsInMongo = mongo.count();
        if(recordsInMongo != recordsInOracle)
        {
            System.err.println(String.format("Number of documents in Mongo is %d but should be %d", recordsInMongo, recordsInOracle));
            System.exit(1);
        }
    }
    
    private static void parseArgs(String[] args)
    {
        try
        {
            numberOfThreads = Integer.parseInt(args[0]);
            oracleDBURL = args[1];
            dBUser = args[2];
            oracleDBPassword = args.length == 3 ? args[2] : args[3];
            System.out.println(String.format("number of threads = %d", numberOfThreads));
            System.out.println(String.format("Oracle DB URL = %s", oracleDBURL));
            System.out.println(String.format("Oracle DB user = %s", dBUser));
            System.out.println(String.format("Oracle DB password = %s", oracleDBPassword));
            System.out.println(String.format("Mongo DB user on localhost default Mongo port = %s", dBUser));
        }
        catch(Throwable e)
        {
            System.err.println("Arguments:");
            System.err.println("1. number of threads: integer. Example : 8");
            System.err.println("2. Oracle DB URL: string. Example : jdbc:oracle:thin:@10.99.81.6:1521:orcl");
            System.err.println("3. Oracle DB user and Mongo DB user (on default Mongo port on localhost): string. Example : VENTES");
            System.err.println("4. Oracle DB password: optional string. Example : VENTES. If missing, it defaults to DB user");
            System.exit(1);
        }
    }
    
    @SuppressWarnings("deprecation")
    private static AuditOracleDataSource oracleDataSource() throws SQLException, IOException
    {
        OracleDataSource db = new OracleDataSource();
        db.setURL(oracleDBURL);
        db.setUser(dBUser);
        db.setPassword(oracleDBPassword);
        db.setConnectionCachingEnabled(true);
        return oracle = new AuditOracleDataSource(db, withoutDossier());
    }
    
    static Set<String> withoutDossier() throws IOException
    {
        try(LineNumberReader reader = new LineNumberReader(new InputStreamReader(Main.class.getResourceAsStream("/commun.lst"))))
        {
            return reader.lines().collect(Collectors.toSet());
        }
    }

    private static AuditMongoDataSink mongo()
    {
        return mongo = new AuditMongoDataSink(dBUser);
    }
    
    private static ThreadPoolExecutor executor()
    {
        return executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(500), new CallerRunsPolicy());
    }
    
    private static Migration migrateToMongo()
    {
        System.out.println(String.format("Number of documents in Oracle : %d", oracle.count()));
        int documentsInMongo = mongo().count();
        System.out.println(String.format("Number of documents in Mongo : %d", documentsInMongo));
        if(documentsInMongo == 0)
        {
            mongo.setIsEmpty();
        }
        return new Migration(oracle, executor(), mongo);
    }
}
