package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class CreateTableTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerate.class);
    private static int THREAD_NUM;
    private static AtomicInteger TOTAL_SIZE = new AtomicInteger();
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;
}
