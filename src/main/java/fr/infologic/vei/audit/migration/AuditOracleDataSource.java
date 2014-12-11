package fr.infologic.vei.audit.migration;

import static fr.infologic.vei.audit.migration.Mesure.IS_10000;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

import oracle.jdbc.pool.OracleDataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

class AuditOracleDataSource implements AuditKeyProducer, AuditDataSource
{
    private final QueryRunner data;

    AuditOracleDataSource(OracleDataSource oracle)
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
    }

    @Override
    public void forEachKey(final Consumer<AuditKey> consumer)
    {
        createIndexIfMissing();
        try
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
                        consumer.accept(key);
                    }
                    return null;
                }
            });
        }
        catch (SQLException e)
        {
            throw new MigrationAuditException(e);
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
            if(key.ek == null)
            {
                return data.query("SELECT count(*) FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK = ? AND sourceEK is null", 
                new ScalarHandler<BigDecimal>(), key.metadataId, key.dossier).intValue();
            }
            else
            {
                return data.query("SELECT count(*) FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK = ? AND sourceEK = ?", 
                                  new ScalarHandler<BigDecimal>(), key.metadataId, key.dossier, key.ek).intValue();
            }
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
            if(key.ek == null)
            {
                result = data.query("SELECT * FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK = ? AND sourceEK is null", 
                new BeanListHandler<>(AuditContent.class), key.metadataId, key.dossier);
            }
            else
            {
                result = data.query("SELECT * FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK = ? AND sourceEK = ?", 
                new BeanListHandler<>(AuditContent.class), key.metadataId, key.dossier, key.ek);
            }
            return result;
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
    
    Mesure mesure = new Mesure("Oracle fetch", "keys", "docs");
}
