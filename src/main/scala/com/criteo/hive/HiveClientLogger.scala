package com.criteo.hive

/**
  * Abstraction for hive client logging.
  */
trait HiveClientLogger {

  def info(str: CharSequence)

  def error(str: CharSequence)

  def debug(str: CharSequence)
}