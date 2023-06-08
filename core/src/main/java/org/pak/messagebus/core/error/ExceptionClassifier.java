package org.pak.messagebus.core.error;

public interface ExceptionClassifier {
    ExceptionType classify(Exception exception);
}
