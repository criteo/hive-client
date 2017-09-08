package com.criteo.hive

case class HiveException(private val message: String = "",
                         private val cause: Throwable = None.orNull)
  extends Exception(message, cause)