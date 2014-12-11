package fr.infologic.vei.audit.migration;

import static fr.infologic.vei.audit.migration.Mesure.IS_10000;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import oracle.jdbc.pool.OracleDataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

class AuditOracleDataSource implements AuditKeyProducer, AuditDataSource
{
    private final QueryRunner data;
    private Set<String> withoutDossier;

    AuditOracleDataSource(OracleDataSource oracle, Set<String> withoutDossier)
    {
        this.data = new QueryRunner(oracle)
        {
            @Override
            protected PreparedStatement prepareStatement(Connection conn, String sql) throws SQLException
            {
                PreparedStatement statement = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                statement.setFetchSize(100);
                return statement;
            }
        };
        this.withoutDossier = withoutDossier;
    }

    @Override
    public void forEachKey(final Consumer<AuditKey> consumer)
    {
        createIndexIfMissing();
        try
        {
            forEachKeyWithDossier(consumer);
            forEachKeyWithoutDossier(consumer);
        }
        catch (SQLException e)
        {
            throw new MigrationAuditException(e);
        }
    }

    private void forEachKeyWithDossier(final Consumer<AuditKey> consumer) throws SQLException
    {
        data.query("SELECT DISTINCT metadataId, sourceDosResIK, sourceEK FROM GL_AuditEntry",
        new BeanHandler<AuditKey>(AuditKey.class)
        {
            @Override
            public AuditKey handle(ResultSet rs) throws SQLException
            {
                AuditKey key;
                while(null != (key = super.handle(rs)))
                {
                    if(!withoutDossier.contains(key.metadataId))
                    {
                        consumer.accept(key);
                    }
                }
                return null;
            }
        });
    }
    
    private void forEachKeyWithoutDossier(final Consumer<AuditKey> consumer) throws SQLException
    {
        for(String metadataId : withoutDossier)
        {
            data.query("SELECT DISTINCT metadataId, sourceEK FROM GL_AuditEntry WHERE metadataId = ?",
            new BeanHandler<AuditKey>(AuditKey.class)
            {
                @Override
                public AuditKey handle(ResultSet rs) throws SQLException
                {
                    AuditKey key;
                    while(null != (key = super.handle(rs)))
                    {
                        consumer.accept(key.withoutDossier());
                    }
                   return null;
               }
           }, metadataId);
        }
    }

    private void createIndexIfMissing()
    {
        try
        {
            data.update("CREATE INDEX audit_entry_migration ON GL_AuditEntry(metadataId, sourceDosResIK, sourceEK)");
        }
        catch (SQLException e)
        {
        }
    }

    @Override
    public int count()
    {
        try
        {
            return data.query("SELECT count(*) FROM GL_AuditEntry", new ScalarHandler<BigDecimal>()).intValue();
        }
        catch (SQLException e)
        {
            throw new MigrationAuditException(e);
        }
    }

    @Override
    public int count(AuditKey key)
    {
        try
        {
            return fetcher(key).count(key);
        }
        catch (SQLException e)
        {
            throw new MigrationAuditException(e);
        }
    }

    @Override
    public List<AuditContent> fetch(AuditKey key)
    {
        mesure.arm();
        List<AuditContent> result = null;
        try
        {
            return result = fetcher(key).fetch(key);
        }
        catch (SQLException e)
        {
            throw new MigrationAuditException(e);
        }
        finally
        {
            mesure.count(1, result == null ? 0 : result.size());
            mesure.printIf(IS_10000);
        }
    }

    private Fetcher fetcher(AuditKey key)
    {
        if(!key.hasDossier())
        {
            if(key.ek == null)
            {
                return NullEKWithoutDossier;
            }
            return EKWithoutDossier;
        }
        if(key.dossier == null)
        {
            if(key.ek == null)
            {
                return NullEKNullDossier;
            }
            return EKNullDossier;
        }
        if(key.ek == null)
        {
            return NullEKDossier;
        }
        return EKDossier;
    }

    Mesure mesure = new Mesure("Oracle fetch", "keys", "docs");
    private interface Fetcher
    {
        int count(AuditKey key) throws SQLException;
        List<AuditContent> fetch(AuditKey key) throws SQLException;
    }
    private Fetcher EKDossier = new Fetcher()
    {
        @Override
        public int count(AuditKey key) throws SQLException
        {
            return data.query("SELECT count(*) FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK = ? AND sourceEK = ?",
            new ScalarHandler<BigDecimal>(), key.metadataId, key.dossier, key.ek).intValue();
        }

        @Override
        public List<AuditContent> fetch(AuditKey key) throws SQLException
        {
            return data.query("SELECT * FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK = ? AND sourceEK = ?",
                              new BeanListHandler<>(AuditContent.class), key.metadataId, key.dossier, key.ek);
        }

    };

    private Fetcher NullEKDossier = new Fetcher()
    {
        @Override
        public int count(AuditKey key) throws SQLException
        {
            return data.query("SELECT count(*) FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK = ? AND sourceEK is null",
                              new ScalarHandler<BigDecimal>(), key.metadataId, key.dossier).intValue();
        }

        @Override
        public List<AuditContent> fetch(AuditKey key) throws SQLException
        {
            return data.query("SELECT * FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK = ? AND sourceEK is null",
                                 new BeanListHandler<>(AuditContent.class), key.metadataId, key.dossier);
        }

    };

    private Fetcher NullEKNullDossier = new Fetcher()
    {
        @Override
        public int count(AuditKey key) throws SQLException
        {
            return data.query("SELECT count(*) FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK is null AND sourceEK is null",
                              new ScalarHandler<BigDecimal>(), key.metadataId).intValue();
        }

        @Override
        public List<AuditContent> fetch(AuditKey key) throws SQLException
        {
            return data.query("SELECT * FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK is null AND sourceEK is null",
                                 new BeanListHandler<>(AuditContent.class), key.metadataId);
        }

    };

    private Fetcher EKNullDossier = new Fetcher()
    {
        @Override
        public int count(AuditKey key) throws SQLException
        {
            return data.query("SELECT count(*) FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK is null AND sourceEK = ?",
                              new ScalarHandler<BigDecimal>(), key.metadataId, key.ek).intValue();
        }

        @Override
        public List<AuditContent> fetch(AuditKey key) throws SQLException
        {
            return data.query("SELECT * FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK is null AND sourceEK = ?",
                                 new BeanListHandler<>(AuditContent.class), key.metadataId, key.ek);
        }

    };

    private Fetcher NullEKWithoutDossier = new Fetcher()
    {
        @Override
        public int count(AuditKey key) throws SQLException
        {
            return data.query("SELECT count(*) FROM GL_AuditEntry WHERE metadataId = ? AND sourceEK is null",
                              new ScalarHandler<BigDecimal>(), key.metadataId).intValue();
        }

        @Override
        public List<AuditContent> fetch(AuditKey key) throws SQLException
        {
            List<AuditContent> result = data.query("SELECT * FROM GL_AuditEntry WHERE metadataId = ? AND sourceEK is null",
                                 new BeanListHandler<>(AuditContent.class), key.metadataId);
            for(AuditContent c : result)
            {
                c.withoutDossier();
            }
            return result;
        }

    };

    private Fetcher EKWithoutDossier = new Fetcher()
    {
        @Override
        public int count(AuditKey key) throws SQLException
        {
            return data.query("SELECT count(*) FROM GL_AuditEntry WHERE metadataId = ? AND sourceEK = ?",
                              new ScalarHandler<BigDecimal>(), key.metadataId, key.ek).intValue();
        }

        @Override
        public List<AuditContent> fetch(AuditKey key) throws SQLException
        {
            List<AuditContent> result = data.query("SELECT * FROM GL_AuditEntry WHERE metadataId = ? AND sourceEK = ?",
                                 new BeanListHandler<>(AuditContent.class), key.metadataId, key.ek);
            for(AuditContent c : result)
            {
                c.withoutDossier();
            }
            return result;
        }

    };
}
