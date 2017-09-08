package com.criteo.hive

import java.net.InetAddress
import java.util.Locale

/**
  * Some security utils.
  */
object SecurityUtil {
  val HOSTNAME_PATTERN = "_HOST"

  private def getComponents(principalConfig: String): Array[String] = {
    if (principalConfig == null) {
      return null
    }
    principalConfig.split("[/@]")
  }

  def replacePattern(components: Array[String], hostname: String) = {
    var fqdn = hostname
    if (fqdn == null || fqdn.isEmpty || fqdn == "0.0.0.0") {
      fqdn = InetAddress.getLocalHost.getCanonicalHostName
    }
    components(0) + "/" + fqdn.toLowerCase(Locale.US) + "@" + components(2)
  }

  def getServerPrincipal(principalConfig: String, hostname: String): String = {
    val components = getComponents(principalConfig)
    if (components == null || components.length != 3 || !components(1).equals(HOSTNAME_PATTERN)) {
      principalConfig
    } else {
      replacePattern(components, hostname)
    }
  }

  def splitKerberosName(fullName: String): Array[String] = fullName.split("[/@]")

}
