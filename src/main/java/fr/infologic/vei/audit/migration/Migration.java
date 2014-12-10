package fr.infologic.vei.audit.migration;

import java.util.concurrent.Executor;
import java.util.function.Consumer;


class Migration implements Consumer<AuditKey>
{
    private final AuditDataSource source;
    private final Executor executor;
    private final AuditDataSink sink;
    
    Migration(AuditDataSource source, Executor executor, AuditDataSink sink)
    {
        this.source = source;
        this.executor = executor;
        this.sink = sink;
    }
    
    @Override
    public void accept(AuditKey key)
    {
        executor.execute(new MigrateKeyTask(key, source, sink));
    }

}
