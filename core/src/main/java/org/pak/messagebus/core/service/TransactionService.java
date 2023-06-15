package org.pak.messagebus.core.service;

import java.util.function.Supplier;

public interface TransactionService {
    <T> T inTransaction(Supplier<T> runnable);
}
