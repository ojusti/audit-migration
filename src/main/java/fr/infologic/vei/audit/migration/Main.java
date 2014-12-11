package fr.infologic.vei.audit.migration;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
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
            throw new RuntimeException(String.format("Number of documents in Mongo is %d but should be %d", recordsInMongo, recordsInOracle));
        }
    }
    
    private static void parseArgs(String[] args)
    {
        numberOfThreads = Integer.parseInt(args[0]);
        oracleDBURL = args[1];
        dBUser = args[2];
        oracleDBPassword = args.length == 3 ? args[2] : args[3];
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
        URL communList = Main.class.getResource("/commun.lst");
        return Files.lines(new File(communList.getFile()).toPath()).collect(Collectors.toSet());
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
