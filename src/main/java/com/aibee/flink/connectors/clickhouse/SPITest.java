package com.aibee.flink.connectors.clickhouse;

import org.apache.flink.table.factories.Factory;

import java.util.ServiceLoader;

public class SPITest {
    public static void main(String[] args) {
        ServiceLoader<Factory> serviceLoader = ServiceLoader.load(Factory.class,Thread.currentThread().getContextClassLoader());
        for (Factory factory : serviceLoader) {
            String identifier = factory.getClass().getCanonicalName();
            System.out.println(identifier);
        }

    }
}
