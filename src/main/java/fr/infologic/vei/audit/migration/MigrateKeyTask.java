package fr.infologic.vei.audit.migration;


class MigrateKeyTask implements Runnable
{
    private final AuditKey key;
    private final AuditDataSource source;
    private final AuditDataSink sink;
    static Mesure mesure = new Mesure("Migrate", "keys", "present", "errors");

    MigrateKeyTask(AuditKey key, AuditDataSource source, AuditDataSink sink)
    {
        this.key = key;
        this.source = source;
        this.sink = sink;
    }

    @Override
    public void run()
    {
        boolean exist = false;
        boolean error = false;
        mesure.arm();
        try
        {
            int imported = sink.count(key);
            if(imported > 0) 
            {
                if(source.count(key) == imported)
                {
                    exist = true;
                    return;
                }
                sink.delete(key);
            }
            error = !importAll();
        }
        finally
        {
            mesure.count(1, exist ? 1 : 0, error ? 1 : 0);
            mesure.printIf(Is_100_000.TESTER);
        }
    }

    private boolean importAll()
    {
        try
        {
            sink.ingest(key, source.fetch(key));
            return true;
        }
        catch(Throwable t)
        {
            System.err.println(String.format("Failed to migrate %s", key));
            t.printStackTrace(System.err);
            return false;
        }
    }

}
