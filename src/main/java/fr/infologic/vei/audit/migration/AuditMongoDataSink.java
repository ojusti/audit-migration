package fr.infologic.vei.audit.migration;

import static fr.infologic.vei.audit.migration.Mesure.IS_10000;

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
    private boolean empty;

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

    void setIsEmpty()
    {
        this.empty = true;
    }
    @Override
    public int count()
    {
        return gateway.db().count();
    }
    
    @Override
    public int count(AuditKey key)
    {
        return empty ? 0 : gateway.find(key).count();
    }

    @Override
    public void delete(AuditKey key)
    {
        gateway.find(key).delete();
    }

    @Override
    public void ingest(AuditKey key, List<AuditContent> trail)
    {
        mesure.arm();
        try
        {
            Collections.sort(trail);
            MongoJson base = new MongoJson(null);
            for(int version = trail.size(); version > 0; version--)
            {
                AuditContent trace = trail.get(version - 1);
                AuditIngestTrace patch = makeTrace(trace);
                patch.version = version;
                MongoJson content = new MongoJson(XMLToMongo.transform(trace.xml()));
                patch.content = content.diff(base);
                gateway.ingest(patch);
                base = content;
            }
            mesure.count(1, trail.size());
        }
        catch(Throwable t)
        {
            gateway.find(key).delete();
            mesure.count(0);
            throw new MigrationAuditException(key, trail, t);
        }
        finally
        {
            mesure.printIf(IS_10000);
        }
    }
    Mesure mesure = new Mesure("Mongo ingest", "keys", "docs");
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
        trace.metadata.put("DOSSIER_CONNEXION", entry.dossier);
        return trace;
    }

}
