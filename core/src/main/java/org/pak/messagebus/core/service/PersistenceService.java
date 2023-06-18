package org.pak.messagebus.core.service;

import org.pak.messagebus.core.error.DuplicateKeyException;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;

public interface PersistenceService {
    void execute(String sql, Object... args);
    int update(String query, Object... args);
    Object insert(String query, Object... args) throws DuplicateKeyException;
    <R> List<R> query(String query, Function<ResultSet, R> resultSetMapper);
}
