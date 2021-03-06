package fr.infologic.vei.audit.migration;

import java.util.List;

interface AuditDataSink
{
    int count();
    int count(AuditKey key);
    void delete(AuditKey key);
    void ingest(AuditKey key, List<AuditContent> trail);

}
