package com.ssafy.witw.infra.kafka.dbwriter;

/** A valid event whose referenced article/issue has not reached the DB yet. */
public class MissingEventDependencyException extends RuntimeException {
    public MissingEventDependencyException(String message) {
        super(message);
    }
}
