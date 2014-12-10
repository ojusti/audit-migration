package fr.infologic.vei.audit.migration;

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
        try
        {
            data.query("SELECT metadataId, sourceDosResIK, sourceEK FROM GL_AuditEntry", 
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

    @Override
    public int count(AuditKey key)
    {
        try
        {
            return data.query("SELECT count(*) FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK = ? AND sourceEK = ?", 
            new ScalarHandler<Integer>(), key.metadataId, key.dossier, key.ek);
        }
        catch (SQLException e)
        {
            throw new MigrationAuditException(e);
        }
    }

    @Override
    public List<AuditContent> fetch(AuditKey key)
    {
        try
        {
            return data.query("SELECT * FROM GL_AuditEntry WHERE metadataId = ? AND sourceDosResIK = ? AND sourceEK = ?", new BeanListHandler<>(AuditContent.class));
        }
        catch (SQLException e)
        {
            throw new MigrationAuditException(e);
        }
    }
}
