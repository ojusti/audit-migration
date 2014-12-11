package fr.infologic.vei.audit.migration;


public class MigrationAuditException extends RuntimeException
{
    MigrationAuditException(Exception e)
    {
        super(e);
    }

    public MigrationAuditException(AuditKey key, Throwable t)
    {
        super(key.toString(), t);
    }

}
