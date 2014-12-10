package fr.infologic.vei.audit.migration;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import fr.infologic.vei.audit.api.AdminDB.AdminDBException;
import fr.infologic.vei.audit.api.AuditIngestTrace;
import fr.infologic.vei.audit.gateway.AuditGateway;
import fr.infologic.vei.audit.gateway.MongoAuditGatewayBuilder;
import fr.infologic.vei.audit.migration.xml.XMLToMongo;
import fr.infologic.vei.audit.mongo.json.MongoJson;

class AuditMongoDataSink implements AuditDataSink
{

    private final AuditGateway gateway;

    AuditMongoDataSink(String dbName)
    {
        try
        {
            gateway = MongoAuditGatewayBuilder.db(dbName).build();
        }
        catch (AdminDBException e)
        {
            throw new MigrationAuditException(e);
        }
    }
    AuditMongoDataSink(AuditGateway gateway)
    {
        this.gateway = gateway;
    }

    @Override
    public int count(AuditKey key)
    {
        return gateway.find(key).count();
    }

    @Override
    public void delete(AuditKey key)
    {
        gateway.find(key).delete();
    }

    @Override
    public void ingest(List<AuditContent> trail)
    {
        Collections.sort(trail);
        MongoJson base = new MongoJson(null);
        for(int version = trail.size(); version > 0; version--)
        {
            AuditContent trace = trail.get(version - 1);
            AuditIngestTrace patch = makeTrace(trace);
            patch.version = version;
            MongoJson content = new MongoJson(XMLToMongo.transform(trace.xml));
            patch.content = content.diff(base);
            gateway.ingest(patch);
            base = content;
        }
    }
    static AuditIngestTrace makeTrace(AuditContent entry)
    {
        AuditIngestTrace trace = new AuditIngestTrace();
        trace.type = entry.getType();
        trace.group = entry.getGroup();
        trace.key = entry.getKey();
        trace.metadata = new HashMap<>();
        trace.metadata.put("DATE_MODIFICATION", entry.dat);
        trace.metadata.put("GUID", entry.guid);
        trace.metadata.put("IK", entry.sourceIK);
        trace.metadata.put("IP", entry.ip);
        trace.metadata.put("LABEL", entry.typ);
        trace.metadata.put("TIMESTAMP", entry.datCre);
        trace.metadata.put("UTILISATEUR", entry.codeUtil);
        return trace;
    }

}
