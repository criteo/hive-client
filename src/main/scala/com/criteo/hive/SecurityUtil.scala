package com.criteo.hive

import java.net.InetAddress
import java.util.Locale

/**
  * Some security utils.
  */
object SecurityUtil {

  def getServerPrincipal(principalConfig: String, hostname: String): String = {
    principalConfig.split("[/@]").toList match {
      case primary :: "_HOST" :: realm :: Nil =>
        val fqdn = hostname match {
          case "" | "0.0.0.0" =>
            InetAddress.getLocalHost.getCanonicalHostName
          case _ =>
            hostname
        }
        s"${ primary }/${fqdn.toLowerCase(Locale.US)}@${realm}"
      case _ =>
        principalConfig

    }
  }

  def splitKerberosName(fullName: String): Array[String] = fullName.split("[/@]")

}
