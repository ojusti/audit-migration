package fr.infologic.vei.audit.migration;

import java.util.List;


public class MigrationAuditException extends RuntimeException
{
    MigrationAuditException(Exception e)
    {
        super(e);
    }

    MigrationAuditException(AuditKey key, List<AuditContent> failure, Throwable t)
    {
        super(String.format("%s: %d records / first is %s", key, failure.size(), failure.get(0)), t);
    }

}
