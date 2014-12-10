package fr.infologic.vei.audit.migration;

import java.util.function.Consumer;

public interface AuditKeyProducer
{
    void forEachKey(Consumer<AuditKey> consumer);
}
