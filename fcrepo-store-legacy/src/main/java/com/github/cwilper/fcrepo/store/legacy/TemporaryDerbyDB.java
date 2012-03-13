package com.github.cwilper.fcrepo.store.legacy;

import com.github.cwilper.fcrepo.store.core.StoreException;
import org.apache.derby.jdbc.EmbeddedDataSource40;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * A {@link JdbcTemplate} configured to use an embedded Derby database that is
 * automatically created at construction time and can be deleted when no
 * longer needed via a call to {@link #delete()}.
 */
public class TemporaryDerbyDB extends JdbcTemplate {
    private final File tempDir;

    private boolean closed;

    public TemporaryDerbyDB() {
        try {
            tempDir = File.createTempFile("fcrepo-store-legacy", null);
            if (!tempDir.delete()) {
                throw new StoreException("Unable to delete temporary file: "
                        + tempDir);
            }
            setDataSource(createDataSource(true));
        } catch (IOException e) {
            throw new StoreException("Error creating temporary file", e);
        } catch (SQLException e) {
            throw new StoreException("Error creating temporary db", e);
        }
    }

    // create if true, shutdown if false
    private EmbeddedDataSource40 createDataSource(boolean create)
            throws SQLException {
        EmbeddedDataSource40 dataSource = new EmbeddedDataSource40();
        dataSource.setDatabaseName(tempDir.toString());
        if (create) {
            dataSource.setCreateDatabase("create");
        } else {
            dataSource.setShutdownDatabase("shutdown");
        }
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.warn("Error closing connection", e);
                }
            }
        }
        return dataSource;
    }

    @PreDestroy
    public void delete() {
        if (!closed) {
            try {
                createDataSource(false);
            } catch (SQLException e) {
                // SQL exception XJ015 is expected
            } finally {
                rmdirs(tempDir);
                closed = true;
            }
        }
    }

    private void rmdirs(File dir) {
        for (File child : dir.listFiles()) {
            if (child.isDirectory()) {
                rmdirs(child);
            } else {
                if (!child.delete()) {
                    logger.warn("Unable to delete file: " + child);
                }
            }
        }
        if (!dir.delete()) {
            logger.warn("Unable to delete dir: " + dir);
        }
    }
}
