package fr.infologic.vei.audit.migration;

class MigrateKeyTask implements Runnable
{

    private final AuditKey key;
    private final AuditDataSource source;
    private final AuditDataSink sink;

    MigrateKeyTask(AuditKey key, AuditDataSource source, AuditDataSink sink)
    {
        this.key = key;
        this.source = source;
        this.sink = sink;
    }

    @Override
    public void run()
    {
        int imported = sink.count(key);
        if(source.count(key) == imported)
        {
            return;
        }
        if(imported > 0) 
        {
            sink.delete(key);
        }
        importAll();
    }

    private void importAll()
    {
        sink.ingest(source.fetch(key));
    }

}
