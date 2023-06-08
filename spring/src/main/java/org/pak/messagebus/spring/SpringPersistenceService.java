package org.pak.messagebus.spring;

import org.pak.messagebus.core.PersistenceService;
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
        jdbcTemplate.execute(con -> con.prepareStatement(query),
                (PreparedStatementCallback<Object>) ps -> {
                    new ArgumentPreparedStatementSetter(args).setValues(ps);
                    ps.execute();
                    return null;
                });
    }

    public int update(String query, Object... args) {
        return jdbcTemplate.update(query, args);
    }

    public Object insert(String query, Object... args) {
        var argumentSetter = new ArgumentPreparedStatementSetter(args);
        var id = jdbcTemplate.execute((PreparedStatementCreator) con -> con.prepareStatement(query, new String[]{"id"}),
                ps -> {
                    argumentSetter.setValues(ps);
                    ps.executeUpdate();
                    ResultSet keys = ps.getGeneratedKeys();
                    try {
                        keys.next();
                        return JdbcUtils.getResultSetValue(keys, 1);
                    } finally {
                        JdbcUtils.closeResultSet(keys);
                    }

                });

        return id;
    }

    @Override
    public <R> List<R> query(String query, Function<ResultSet, R> mapper) {
        return jdbcTemplate.query(query, (rs, rowNum) -> mapper.apply(rs));
    }
}
