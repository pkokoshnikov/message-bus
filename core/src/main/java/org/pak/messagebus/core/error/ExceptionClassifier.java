package org.pak.messagebus.core.error;

public interface ExceptionClassifier {
    boolean isBlockedException(Exception exception);
    boolean isNonRetryableException(Exception exception);
}
