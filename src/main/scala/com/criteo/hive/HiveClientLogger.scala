package com.criteo.hive

/**
  * Abstraction for hive client logging.
  */
trait HiveClientLogger {

  def info(line: => String)

  def error(line: => String)

  def debug(line: => String)
}