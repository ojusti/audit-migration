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
        super(key + ": " + failure, t);
    }

}
