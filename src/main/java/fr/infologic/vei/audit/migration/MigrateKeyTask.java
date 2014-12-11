package fr.infologic.vei.audit.migration;

import static fr.infologic.vei.audit.migration.Mesure.IS_10000;

class MigrateKeyTask implements Runnable
{

    private final AuditKey key;
    private final AuditDataSource source;
    private final AuditDataSink sink;
    private static Mesure mesure = new Mesure("Migrate", "keys", "imported", "deleted");

    MigrateKeyTask(AuditKey key, AuditDataSource source, AuditDataSink sink)
    {
        this.key = key;
        this.source = source;
        this.sink = sink;
    }

    @Override
    public void run()
    {
        boolean fastExit = false;
        boolean deleted = false;
        mesure.arm();
        try
        {
            int imported = sink.count(key);
            if(imported > 0) 
            {
                if(source.count(key) == imported)
                {
                    fastExit = true;
                    return;
                }
                deleted = true;
                sink.delete(key);
            }
            importAll();
        }
        finally
        {
            mesure.count(1, fastExit ? 0 : 1, deleted ? 1 : 0);
            mesure.printIf(IS_10000);
        }
    }

    private void importAll()
    {
        try
        {
            sink.ingest(source.fetch(key));
        }
        catch(Throwable t)
        {
            System.out.println(source.fetch(key));
            throw new MigrationAuditException(key, t);
        }
    }

}
