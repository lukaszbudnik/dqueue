package com.github.lukaszbudnik.dqueue;

@FunctionalInterface
interface NoArgFunction<R> {
    R apply() throws Exception;
}

@FunctionalInterface
interface NoArgVoidFunction<R> {
    void apply() throws Exception;
}
