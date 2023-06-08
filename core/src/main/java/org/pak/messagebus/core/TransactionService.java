package org.pak.messagebus.core;

import java.util.function.Supplier;

public interface TransactionService {
    <T> T inTransaction(Supplier<T> runnable);
}
