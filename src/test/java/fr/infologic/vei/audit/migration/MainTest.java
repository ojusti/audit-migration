package fr.infologic.vei.audit.migration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;

import oracle.jdbc.pool.OracleDataSource;

import org.junit.Ignore;
import org.junit.Test;

import fr.infologic.vei.audit.api.AuditIngestTrace;
@Ignore
public class MainTest extends AuditGatewayStub
{
    private static final String URL = "jdbc:oracle:thin:@10.99.81.6:1521:orcl";
    private static final String USER = "RAN_VT_VALENTIN";
    private static final String PASSWORD = "RAN_VT_VALENTIN";
    
    @Test
    public void ingestAll() throws SQLException, InterruptedException, IOException
    {
        Main.main("8", URL, USER, PASSWORD);
    }
    
    @Test
    public void ingestNightlyBuild() throws SQLException
    {
        OracleDataSource db = new OracleDataSource();
        db.setURL("jdbc:oracle:thin:@10.99.81.19:1521:orclweiso");
        db.setUser("MI_VALENTIN_HEAD");
        db.setPassword("MI_VALENTIN_HEAD");
        AuditKey key = new AuditKey();
        key.setMetadataId("fr.infologic.stocks.cumuls.modele.Prevision");
        key.setSourceDosResIK(1L);
        key.setSourceEK("02J");
        new AuditMongoDataSink("MI_VALENTIN_HEAD").ingest(key, new AuditOracleDataSource(db, Collections.emptySet()).fetch(key));
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
        new AuditMongoDataSink(this).ingest(key, new AuditOracleDataSource(db, Collections.emptySet()).fetch(key));
    }

    @Override
    public void ingest(AuditIngestTrace patch)
    {
        
    }
    
    @Test
    public void testParse()
    {
        double x = new Double("36.000000");
        System.out.println(x);
    }
}
