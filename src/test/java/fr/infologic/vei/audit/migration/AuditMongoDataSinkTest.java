package fr.infologic.vei.audit.migration;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.Before;
import org.junit.Test;

import fr.infologic.vei.audit.api.AuditIngestTrace;
import fr.infologic.vei.audit.mongo.json.MongoJson;


public class AuditMongoDataSinkTest extends AuditGatewayStub
{
    private List<AuditContent> content;
    private long sequence;
    private List<AuditIngestTrace> patches;
    
    @Test
    public void testIngest()
    {
        new AuditMongoDataSink(this).ingest(content.get(0), content);
        assertAllPatchesWereIngested();
    }

    private void assertAllPatchesWereIngested()
    {
        Condition<AuditIngestTrace> allValuesNull = new Condition<AuditIngestTrace>()
        {
            @Override public boolean matches(AuditIngestTrace value) { return value == null; }
        }.describedAs("null values");
        Assertions.assertThat(patches).have(allValuesNull);
    }
    
    @Before
    public void setUp()
    {
        sequence = 10000;
        content = new ArrayList<>();
        patches = new ArrayList<>();
        makeEntry("<a>1</a>", "{}");
        makeEntry("<a>1</a>", "{a:1}");
        makeEntry("<a>2</a>", "{a:2}");
        makeEntry(null, "{}");
        Collections.shuffle(content);
    }

    private void makeEntry(String xml, String json)
    {
        AuditContent entry = entry(xml);
        content.add(entry);
        AuditIngestTrace trace = AuditMongoDataSink.makeTrace(entry);
        trace.content = MongoJson.fromString(json);
        patches.add(trace);
        trace.version = patches.size();
    }

    private AuditContent entry(final String xml)
    {
        AuditContent entry = new AuditContent()
        {
            @Override
            String xml()
            {
                return xml == null ? "<object/>" : document(xml);
            }
        };
        entry.setMetadataId("type");
        entry.setSourceDosResIK(1L);
        entry.setSourceEK("key");
        entry.setSourceIK(2L);
        entry.setIk(sequence += 1000);
        entry.setDat(new Timestamp(sequence));
        entry.setDatCre(entry.dat);
        entry.setGuid(String.valueOf(sequence));
        entry.setCodeUtil(entry.guid);
        entry.setIp(entry.guid);
        if(xml == null)
        {
            entry.setTyp(2);
        }
        else
        {
            entry.setTyp(1);
        }
        return entry;
    }

    private static String document(String content) 
    {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><object class=\"fr.infologic.stocks.fichierbase.modele.Produit\">" + content + "</object>";
    }

    @Override
    public void ingest(AuditIngestTrace patch)
    {
        Assertions.assertThat(patch).isEqualToComparingFieldByField(patches.get(patch.version - 1));
        patches.set(patch.version - 1, null);
    }

}
