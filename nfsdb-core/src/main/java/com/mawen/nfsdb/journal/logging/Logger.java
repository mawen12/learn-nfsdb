package com.mawen.nfsdb.journal.logging;

import java.util.logging.Level;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/4/23
 */
public class Logger {
	private final java.util.logging.Logger logger;

	/////////////////////////////////////////////////////////////////

	public static Logger getLogger(Class<?> clazz) {
		return new Logger(clazz);
	}

	/////////////////////////////////////////////////////////////////

	public void trace(Object message) {
		logger.finest(message.toString());
	}

	public void trace(String format, Object... args) {
		if (isTraceEnabled()) {
			logger.finest(String.format(format, args));
		}
	}

	public boolean isTraceEnabled() {
		return logger.isLoggable(Level.FINEST);
	}

	/////////////////////////////////////////////////////////////////

	public void debug(Object message) {
		logger.fine(message.toString());
	}

	public void debug(Object message, Throwable throwable) {
		logger.log(Level.FINE, message.toString(), throwable);
	}

	public void debug(String format, Object... args) {
		if (isDebugEnabled()) {
			logger.log(Level.FINE, String.format(format, args));
		}
	}

	public void debug(String format, Throwable throwable, Object... args) {
		if (isDebugEnabled()) {
			logger.log(Level.FINE, String.format(format, args), throwable);
		}
	}

	public boolean isDebugEnabled() {
		return logger.isLoggable(Level.FINE);
	}

	/////////////////////////////////////////////////////////////////

	public void info(Object message) {
		logger.info(message.toString());
	}

	public void info(Object message, Throwable throwable) {
		logger.log(Level.INFO, message.toString(), throwable);
	}

	public void info(String format, Object... args) {
		if (isInfoEnabled()) {
			logger.info(String.format(format, args));
		}
	}

	public void info(String format, Throwable throwable, Object... args) {
		logger.log(Level.INFO, String.format(format, args), throwable);
	}

	public boolean isInfoEnabled() {
		return logger.isLoggable(Level.INFO);
	}

	/////////////////////////////////////////////////////////////////

	public void warn(Object message) {
		logger.warning(message.toString());
	}

	/////////////////////////////////////////////////////////////////

	public void error(Object message) {
		logger.severe(message.toString());
	}

	public void error(Object message, Throwable throwable) {
		logger.log(Level.SEVERE, message.toString(), throwable);
	}

	public void error(String format, Object... args) {
		logger.log(Level.SEVERE, String.format(format, args));
	}

	public void error(String format, Throwable throwable, Object... args) {
		logger.log(Level.SEVERE, String.format(format, args), throwable);
	}

	public boolean isErrorEnabled() {
		return logger.isLoggable(Level.SEVERE);
	}

	/////////////////////////////////////////////////////////////////

	private Logger(Class<?> aClass) {
		logger = java.util.logging.Logger.getLogger(aClass.getName());
	}
}
