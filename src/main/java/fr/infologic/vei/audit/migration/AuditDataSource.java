package fr.infologic.vei.audit.migration;

import java.util.List;

interface AuditDataSource
{
    int count();
    int count(AuditKey key);

    List<AuditContent> fetch(AuditKey key);

}
