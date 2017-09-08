package com.criteo.hive

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation
import org.apache.thrift.transport.TTransport

/**
  * This transport is used by SASL Kerberos client.
  */
class TUGIAssumingTransport(wrapped: TTransport, ugi: UserGroupInformation) extends TTransport {

  def open: Unit = {
    ugi.doAs(new PrivilegedExceptionAction[Unit]() {
      def run() {
        wrapped.open();
      }
    });
  }

  def isOpen: Boolean = wrapped.isOpen

  override def peek: Boolean = wrapped.peek

  def close() {
    wrapped.close()
  }


  def read(buf: Array[Byte], off: Int, len: Int): Int = wrapped.read(buf, off, len)

  override def readAll(buf: Array[Byte], off: Int, len: Int): Int = wrapped.readAll(buf, off, len)

  override def write(buf: Array[Byte]) {
    wrapped.write(buf)
  }

  def write(buf: Array[Byte], off: Int, len: Int) {
    wrapped.write(buf, off, len)
  }

  override def flush() {
    wrapped.flush()
  }

  override def getBuffer: Array[Byte] = wrapped.getBuffer

  override def getBufferPosition: Int = wrapped.getBufferPosition

  override def getBytesRemainingInBuffer: Int = wrapped.getBytesRemainingInBuffer

  override def consumeBuffer(len: Int) {
    wrapped.consumeBuffer(len)
  }
}
