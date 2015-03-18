/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BusiTypeSchemaColumn implements org.apache.thrift.TBase<BusiTypeSchemaColumn, BusiTypeSchemaColumn._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("BusiTypeSchemaColumn");

  private static final org.apache.thrift.protocol.TField BUSI_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("busiType", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField SCHEMA_FIELD_DESC = new org.apache.thrift.protocol.TField("schema", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField COLUMN_FIELD_DESC = new org.apache.thrift.protocol.TField("column", org.apache.thrift.protocol.TType.STRING, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new BusiTypeSchemaColumnStandardSchemeFactory());
    schemes.put(TupleScheme.class, new BusiTypeSchemaColumnTupleSchemeFactory());
  }

  private Busitype busiType; // required
  private GlobalSchema schema; // required
  private String column; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BUSI_TYPE((short)1, "busiType"),
    SCHEMA((short)2, "schema"),
    COLUMN((short)3, "column");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // BUSI_TYPE
          return BUSI_TYPE;
        case 2: // SCHEMA
          return SCHEMA;
        case 3: // COLUMN
          return COLUMN;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BUSI_TYPE, new org.apache.thrift.meta_data.FieldMetaData("busiType", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Busitype.class)));
    tmpMap.put(_Fields.SCHEMA, new org.apache.thrift.meta_data.FieldMetaData("schema", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, GlobalSchema.class)));
    tmpMap.put(_Fields.COLUMN, new org.apache.thrift.meta_data.FieldMetaData("column", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(BusiTypeSchemaColumn.class, metaDataMap);
  }

  public BusiTypeSchemaColumn() {
  }

  public BusiTypeSchemaColumn(
    Busitype busiType,
    GlobalSchema schema,
    String column)
  {
    this();
    this.busiType = busiType;
    this.schema = schema;
    this.column = column;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public BusiTypeSchemaColumn(BusiTypeSchemaColumn other) {
    if (other.isSetBusiType()) {
      this.busiType = new Busitype(other.busiType);
    }
    if (other.isSetSchema()) {
      this.schema = new GlobalSchema(other.schema);
    }
    if (other.isSetColumn()) {
      this.column = other.column;
    }
  }

  public BusiTypeSchemaColumn deepCopy() {
    return new BusiTypeSchemaColumn(this);
  }

  @Override
  public void clear() {
    this.busiType = null;
    this.schema = null;
    this.column = null;
  }

  public Busitype getBusiType() {
    return this.busiType;
  }

  public void setBusiType(Busitype busiType) {
    this.busiType = busiType;
  }

  public void unsetBusiType() {
    this.busiType = null;
  }

  /** Returns true if field busiType is set (has been assigned a value) and false otherwise */
  public boolean isSetBusiType() {
    return this.busiType != null;
  }

  public void setBusiTypeIsSet(boolean value) {
    if (!value) {
      this.busiType = null;
    }
  }

  public GlobalSchema getSchema() {
    return this.schema;
  }

  public void setSchema(GlobalSchema schema) {
    this.schema = schema;
  }

  public void unsetSchema() {
    this.schema = null;
  }

  /** Returns true if field schema is set (has been assigned a value) and false otherwise */
  public boolean isSetSchema() {
    return this.schema != null;
  }

  public void setSchemaIsSet(boolean value) {
    if (!value) {
      this.schema = null;
    }
  }

  public String getColumn() {
    return this.column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public void unsetColumn() {
    this.column = null;
  }

  /** Returns true if field column is set (has been assigned a value) and false otherwise */
  public boolean isSetColumn() {
    return this.column != null;
  }

  public void setColumnIsSet(boolean value) {
    if (!value) {
      this.column = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case BUSI_TYPE:
      if (value == null) {
        unsetBusiType();
      } else {
        setBusiType((Busitype)value);
      }
      break;

    case SCHEMA:
      if (value == null) {
        unsetSchema();
      } else {
        setSchema((GlobalSchema)value);
      }
      break;

    case COLUMN:
      if (value == null) {
        unsetColumn();
      } else {
        setColumn((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case BUSI_TYPE:
      return getBusiType();

    case SCHEMA:
      return getSchema();

    case COLUMN:
      return getColumn();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case BUSI_TYPE:
      return isSetBusiType();
    case SCHEMA:
      return isSetSchema();
    case COLUMN:
      return isSetColumn();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof BusiTypeSchemaColumn)
      return this.equals((BusiTypeSchemaColumn)that);
    return false;
  }

  public boolean equals(BusiTypeSchemaColumn that) {
    if (that == null)
      return false;

    boolean this_present_busiType = true && this.isSetBusiType();
    boolean that_present_busiType = true && that.isSetBusiType();
    if (this_present_busiType || that_present_busiType) {
      if (!(this_present_busiType && that_present_busiType))
        return false;
      if (!this.busiType.equals(that.busiType))
        return false;
    }

    boolean this_present_schema = true && this.isSetSchema();
    boolean that_present_schema = true && that.isSetSchema();
    if (this_present_schema || that_present_schema) {
      if (!(this_present_schema && that_present_schema))
        return false;
      if (!this.schema.equals(that.schema))
        return false;
    }

    boolean this_present_column = true && this.isSetColumn();
    boolean that_present_column = true && that.isSetColumn();
    if (this_present_column || that_present_column) {
      if (!(this_present_column && that_present_column))
        return false;
      if (!this.column.equals(that.column))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_busiType = true && (isSetBusiType());
    builder.append(present_busiType);
    if (present_busiType)
      builder.append(busiType);

    boolean present_schema = true && (isSetSchema());
    builder.append(present_schema);
    if (present_schema)
      builder.append(schema);

    boolean present_column = true && (isSetColumn());
    builder.append(present_column);
    if (present_column)
      builder.append(column);

    return builder.toHashCode();
  }

  public int compareTo(BusiTypeSchemaColumn other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    BusiTypeSchemaColumn typedOther = (BusiTypeSchemaColumn)other;

    lastComparison = Boolean.valueOf(isSetBusiType()).compareTo(typedOther.isSetBusiType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBusiType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.busiType, typedOther.busiType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSchema()).compareTo(typedOther.isSetSchema());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSchema()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.schema, typedOther.schema);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetColumn()).compareTo(typedOther.isSetColumn());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColumn()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.column, typedOther.column);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("BusiTypeSchemaColumn(");
    boolean first = true;

    sb.append("busiType:");
    if (this.busiType == null) {
      sb.append("null");
    } else {
      sb.append(this.busiType);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("schema:");
    if (this.schema == null) {
      sb.append("null");
    } else {
      sb.append(this.schema);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("column:");
    if (this.column == null) {
      sb.append("null");
    } else {
      sb.append(this.column);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (busiType != null) {
      busiType.validate();
    }
    if (schema != null) {
      schema.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class BusiTypeSchemaColumnStandardSchemeFactory implements SchemeFactory {
    public BusiTypeSchemaColumnStandardScheme getScheme() {
      return new BusiTypeSchemaColumnStandardScheme();
    }
  }

  private static class BusiTypeSchemaColumnStandardScheme extends StandardScheme<BusiTypeSchemaColumn> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, BusiTypeSchemaColumn struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // BUSI_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.busiType = new Busitype();
              struct.busiType.read(iprot);
              struct.setBusiTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SCHEMA
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.schema = new GlobalSchema();
              struct.schema.read(iprot);
              struct.setSchemaIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // COLUMN
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.column = iprot.readString();
              struct.setColumnIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, BusiTypeSchemaColumn struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.busiType != null) {
        oprot.writeFieldBegin(BUSI_TYPE_FIELD_DESC);
        struct.busiType.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.schema != null) {
        oprot.writeFieldBegin(SCHEMA_FIELD_DESC);
        struct.schema.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.column != null) {
        oprot.writeFieldBegin(COLUMN_FIELD_DESC);
        oprot.writeString(struct.column);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class BusiTypeSchemaColumnTupleSchemeFactory implements SchemeFactory {
    public BusiTypeSchemaColumnTupleScheme getScheme() {
      return new BusiTypeSchemaColumnTupleScheme();
    }
  }

  private static class BusiTypeSchemaColumnTupleScheme extends TupleScheme<BusiTypeSchemaColumn> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, BusiTypeSchemaColumn struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetBusiType()) {
        optionals.set(0);
      }
      if (struct.isSetSchema()) {
        optionals.set(1);
      }
      if (struct.isSetColumn()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetBusiType()) {
        struct.busiType.write(oprot);
      }
      if (struct.isSetSchema()) {
        struct.schema.write(oprot);
      }
      if (struct.isSetColumn()) {
        oprot.writeString(struct.column);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, BusiTypeSchemaColumn struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.busiType = new Busitype();
        struct.busiType.read(iprot);
        struct.setBusiTypeIsSet(true);
      }
      if (incoming.get(1)) {
        struct.schema = new GlobalSchema();
        struct.schema.read(iprot);
        struct.setSchemaIsSet(true);
      }
      if (incoming.get(2)) {
        struct.column = iprot.readString();
        struct.setColumnIsSet(true);
      }
    }
  }

}
