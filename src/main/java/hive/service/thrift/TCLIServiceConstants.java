/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package hive.service.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
public class TCLIServiceConstants {

  public static final Set<TTypeId> PRIMITIVE_TYPES = new HashSet<TTypeId>();
  static {
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.BOOLEAN_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.TINYINT_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.SMALLINT_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.INT_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.BIGINT_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.FLOAT_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.DOUBLE_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.STRING_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.TIMESTAMP_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.BINARY_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.DECIMAL_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.NULL_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.DATE_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.VARCHAR_TYPE);
    PRIMITIVE_TYPES.add(hive.service.thrift.TTypeId.CHAR_TYPE);
  }

  public static final Set<TTypeId> COMPLEX_TYPES = new HashSet<TTypeId>();
  static {
    COMPLEX_TYPES.add(hive.service.thrift.TTypeId.ARRAY_TYPE);
    COMPLEX_TYPES.add(hive.service.thrift.TTypeId.MAP_TYPE);
    COMPLEX_TYPES.add(hive.service.thrift.TTypeId.STRUCT_TYPE);
    COMPLEX_TYPES.add(hive.service.thrift.TTypeId.UNION_TYPE);
    COMPLEX_TYPES.add(hive.service.thrift.TTypeId.USER_DEFINED_TYPE);
  }

  public static final Set<TTypeId> COLLECTION_TYPES = new HashSet<TTypeId>();
  static {
    COLLECTION_TYPES.add(hive.service.thrift.TTypeId.ARRAY_TYPE);
    COLLECTION_TYPES.add(hive.service.thrift.TTypeId.MAP_TYPE);
  }

  public static final Map<TTypeId,String> TYPE_NAMES = new HashMap<TTypeId,String>();
  static {
    TYPE_NAMES.put(hive.service.thrift.TTypeId.BOOLEAN_TYPE, "BOOLEAN");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.TINYINT_TYPE, "TINYINT");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.SMALLINT_TYPE, "SMALLINT");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.INT_TYPE, "INT");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.BIGINT_TYPE, "BIGINT");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.FLOAT_TYPE, "FLOAT");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.DOUBLE_TYPE, "DOUBLE");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.STRING_TYPE, "STRING");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.TIMESTAMP_TYPE, "TIMESTAMP");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.BINARY_TYPE, "BINARY");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.ARRAY_TYPE, "ARRAY");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.MAP_TYPE, "MAP");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.STRUCT_TYPE, "STRUCT");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.UNION_TYPE, "UNIONTYPE");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.DECIMAL_TYPE, "DECIMAL");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.NULL_TYPE, "NULL");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.DATE_TYPE, "DATE");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.VARCHAR_TYPE, "VARCHAR");
    TYPE_NAMES.put(hive.service.thrift.TTypeId.CHAR_TYPE, "CHAR");
  }

  public static final String CHARACTER_MAXIMUM_LENGTH = "characterMaximumLength";

  public static final String PRECISION = "precision";

  public static final String SCALE = "scale";

}