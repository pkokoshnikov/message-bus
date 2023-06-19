package org.pak.messagebus.spring;

import org.pak.messagebus.core.error.DuplicateKeyException;
import org.pak.messagebus.core.error.PersistenceException;
import org.pak.messagebus.core.service.PersistenceService;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.JdbcUtils;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;

public class SpringPersistenceService implements PersistenceService {
    private final JdbcTemplate jdbcTemplate;

    public SpringPersistenceService(
            JdbcTemplate jdbcTemplate
    ) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void execute(String query, Object... args) {
        try {
            jdbcTemplate.execute(con -> con.prepareStatement(query),
                    (PreparedStatementCallback<Object>) ps -> {
                        new ArgumentPreparedStatementSetter(args).setValues(ps);
                        ps.execute();
                        return null;
                    });
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    public int update(String query, Object... args) {
        try {
            return jdbcTemplate.update(query, args);
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    public Object insert(String query, Object... args) throws DuplicateKeyException {
        try {
            return jdbcTemplate.execute(
                    (PreparedStatementCreator) con -> con.prepareStatement(query, new String[]{"id"}),
                    ps -> {
                        new ArgumentPreparedStatementSetter(args).setValues(ps);
                        ps.executeUpdate();
                        ResultSet keys = ps.getGeneratedKeys();
                        try {
                            keys.next();
                            return JdbcUtils.getResultSetValue(keys, 1);
                        } finally {
                            JdbcUtils.closeResultSet(keys);
                        }
                    });
        } catch (org.springframework.dao.DuplicateKeyException exception) {
            throw new DuplicateKeyException();
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public <R> List<R> query(String query, Function<ResultSet, R> mapper) {
        try {
            return jdbcTemplate.query(query, (rs, rowNum) -> mapper.apply(rs));
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }
}
