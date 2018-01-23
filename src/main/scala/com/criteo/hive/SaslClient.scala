package com.criteo.hive

import collection.JavaConversions._
import java.util.Base64
import javax.security.auth.callback.{Callback, CallbackHandler, NameCallback, PasswordCallback}
import javax.security.sasl.{RealmCallback}

import org.apache.hadoop.security.SaslRpcServer.AuthMethod
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.thrift.transport.{TSaslClientTransport, TTransport}

/**
  * Logic for the SASL Kerberos client
  */
object SaslClient {

  def encodeIdentifier(identifier: Array[Byte]): String =
    new String(Base64.getEncoder.encode(identifier))

  def encodePassword(password: Array[Byte]): Array[Char] =
    new String(Base64.getEncoder.encode(password)).toCharArray

  private class SaslClientCallbackHandler[T <: TokenIdentifier] (val token: Token[T]) extends CallbackHandler {
    val userName = encodeIdentifier(token.getIdentifier)
    val userPassword = encodePassword(token.getPassword)


    def handle(callbacks: Array[Callback]) {
      var nc = null
      var pc = null
      var rc = null
      callbacks.foreach { c => {
        c match {
          case rc: RealmCallback => rc.setText(rc.getDefaultText())
          case nc: NameCallback => nc.setName(userName)
          case pc: PasswordCallback => pc.setPassword(userPassword)
          case _ => throw new UnsupportedOperationException("Unrecognized SASL client callback" + c)
        }
      }
      }
    }
  }


  def createClientTransport(principalConfig: String,
                            host: String,
                            methodStr: String,
                            tokenStrForm: String,
                            underlyingTransport: TTransport,
                            saslProps: Map[String, String],
                            user: UserGroupInformation): TTransport = {

    methodStr match {
      case "KERBEROS" => {
        val serverPrincipal = SecurityUtil.getServerPrincipal(principalConfig, host)
        val names = SecurityUtil.splitKerberosName(serverPrincipal)
        require(names.length == 3,
          "Kerberos principal name does NOT have the expected hostname part: " + serverPrincipal)
        val saslTransport = new TSaslClientTransport(
          AuthMethod.KERBEROS.getMechanismName,
          null,
          names(0),
          names(1),
          mapAsJavaMap(saslProps),
          null,
          underlyingTransport)
        new TUGIAssumingTransport(saslTransport, user)
      }
      case _ => throw new UnsupportedOperationException("Unsupported method")
    }
  }
}
