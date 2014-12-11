package fr.infologic.vei.audit.migration;

import java.sql.SQLException;

import oracle.jdbc.pool.OracleDataSource;

import org.junit.Ignore;
import org.junit.Test;

import fr.infologic.vei.audit.api.AuditIngestTrace;


public class MainTest extends AuditGatewayStub
{
    private static final String URL = "jdbc:oracle:thin:@10.99.81.6:1521:orcl";
    private static final String USER = "RAN_VT_VALENTIN";
    private static final String PASSWORD = "RAN_VT_VALENTIN";
    
    @Test
    public void ingestAll() throws SQLException, InterruptedException
    {
        Main.main("8", URL, USER, PASSWORD);
    }
    
    @Ignore @Test
    public void unzip() throws SQLException
    {
        OracleDataSource db = new OracleDataSource();
        db.setURL(URL);
        db.setUser(USER);
        db.setPassword(PASSWORD);
        AuditKey key = new AuditKey();
        key.setMetadataId("fr.infologic.gpao.modele.Planning");
        key.setSourceDosResIK(2L);
        key.setSourceEK("SEM");
        new AuditMongoDataSink(this).ingest(new AuditOracleDataSource(db).fetch(key));
    }

    @Override
    public void ingest(AuditIngestTrace patch)
    {
        
    }
}
