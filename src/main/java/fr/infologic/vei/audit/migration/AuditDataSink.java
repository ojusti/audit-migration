package fr.infologic.vei.audit.migration;

import java.util.List;

interface AuditDataSink
{
    int count(AuditKey key);
    void delete(AuditKey key);
    void ingest(List<AuditContent> content);

}
