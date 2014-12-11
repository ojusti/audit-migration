package fr.infologic.vei.audit.migration;

import static org.junit.Assert.fail;
import fr.infologic.vei.audit.api.AdminDB;
import fr.infologic.vei.audit.api.AuditIngestTrace;
import fr.infologic.vei.audit.api.AuditTrace;
import fr.infologic.vei.audit.api.TrailKey;
import fr.infologic.vei.audit.gateway.AuditGateway;

public class AuditGatewayStub implements AuditGateway
{
    @Override public void ingest(AuditIngestTrace patch) { fail("Should not be called"); }
    @Override public void trace(AuditTrace trace) { fail("Should not be called"); }
    @Override public TrailFind find(TrailKey key) { fail("Should not be called"); return null; }
    @Override public TraceQueryDispatch makeQuery() { fail("Should not be called"); return null; }
    @Override public AdminDB db() { fail("Should not be called"); return null; }
}
