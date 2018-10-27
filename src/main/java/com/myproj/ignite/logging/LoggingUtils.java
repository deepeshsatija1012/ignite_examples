package com.myproj.ignite.logging;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LoggingUtils {
    private static final String IDENTIFIER_PREFIX_INFO = "{} - {}";

    private static final Map<Class<?>, Logger> LOGGER_MAP = new ConcurrentHashMap<>();

    public static Logger addLogger(Class<?> clazz){
        LOGGER_MAP.putIfAbsent(clazz, LoggerFactory.getLogger(clazz));
        return LOGGER_MAP.get(clazz);
    }

    private static void info(Logger logger, String id, String message){
        logger.info(IDENTIFIER_PREFIX_INFO, id, message);
    }

    private static void warn(Logger logger, String id, String message){
        logger.warn(IDENTIFIER_PREFIX_INFO, id, message);
    }

    private static void warn(Logger logger, String id, FormattingTuple tuple){
        if(tuple.getThrowable()==null){
            warn(logger, id, tuple.getMessage(), tuple.getThrowable());
        }else {
            warn(logger, id, tuple.getMessage());
        }
    }

    private static void warn(Logger logger, String id, String message, Throwable t){
        logger.warn(IDENTIFIER_PREFIX_INFO, id, message, t);
    }

    private static void error(Logger logger, String id, String message){
        logger.error(IDENTIFIER_PREFIX_INFO, id, message);
    }

    private static void error(Logger logger, String id, String message, Throwable t){
        logger.error(IDENTIFIER_PREFIX_INFO, id, message, t);
    }

    public static void info(Class<?> clazz, String id, String message){
        info(LOGGER_MAP.get(clazz), id, message);
    }

    public static void info(Class<?> clazz, String id, String message, Object arg1){
        info(LOGGER_MAP.get(clazz), id, MessageFormatter.format(message, arg1).getMessage());
    }

    public static void info(Class<?> clazz, String id, String message, Object arg1, Object arg2){
        info(LOGGER_MAP.get(clazz), id, MessageFormatter.format(message, arg1, arg2).getMessage());
    }

    public static void info(Class<?> clazz, String id, String message, Object... args){
        if(ArrayUtils.isEmpty(args)){
            info(clazz, id, message);
            return;
        }
        Object[] objectArgs = args;
        info(LOGGER_MAP.get(clazz), id, MessageFormatter.format(message, objectArgs).getMessage());
    }

    public static void warn(Class<?> clazz, String id, String message){
        warn(LOGGER_MAP.get(clazz), id, message);
    }

    public static void warn(Class<?> clazz, String id, String message, Object arg1){
        warn(LOGGER_MAP.get(clazz), id, MessageFormatter.format(message, arg1));
    }

    public static void warn(Class<?> clazz, String id, String message, Object arg1, Object arg2){
        warn(LOGGER_MAP.get(clazz), id, MessageFormatter.format(message, arg1, arg2));
    }

    public static void warn(Class<?> clazz, String id, String message, Object... args){
        if(ArrayUtils.isEmpty(args)){
            info(clazz, id, message);
            return;
        }
        Object[] objectArgs = args;
        warn(LOGGER_MAP.get(clazz), id, MessageFormatter.format(message, objectArgs));
    }

    public static void error(Class<?> clazz, String id, String message){
        error(LOGGER_MAP.get(clazz), id, message);
    }

    public static void error(Class<?> clazz, String id, String message, Object arg1, Throwable t){
        error(LOGGER_MAP.get(clazz), id, MessageFormatter.format(message, arg1).getMessage(), t);
    }

    public static void error(Class<?> clazz, String id, String message, Object arg1, Object arg2, Throwable t){
        error(LOGGER_MAP.get(clazz), id, MessageFormatter.format(message, arg1, arg2).getMessage(), t);
    }

    public static void error(Class<?> clazz, String id, String message, Object... args){
        if(ArrayUtils.isEmpty(args)){
            info(clazz, id, message);
            return;
        }
        Object[] objectArgs = args;
        FormattingTuple tuple = MessageFormatter.format(message, objectArgs);
        error(LOGGER_MAP.get(clazz), id, tuple.getMessage(), tuple.getThrowable());
    }


}
