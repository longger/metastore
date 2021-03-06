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

public class SplitValue implements org.apache.thrift.TBase<SplitValue, SplitValue._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SplitValue");

  private static final org.apache.thrift.protocol.TField SPLIT_KEY_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("splitKeyName", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField LEVEL_FIELD_DESC = new org.apache.thrift.protocol.TField("level", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField VALUE_FIELD_DESC = new org.apache.thrift.protocol.TField("value", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField VERISON_FIELD_DESC = new org.apache.thrift.protocol.TField("verison", org.apache.thrift.protocol.TType.I64, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SplitValueStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SplitValueTupleSchemeFactory());
  }

  private String splitKeyName; // required
  private int level; // required
  private String value; // required
  private long verison; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SPLIT_KEY_NAME((short)1, "splitKeyName"),
    LEVEL((short)2, "level"),
    VALUE((short)3, "value"),
    VERISON((short)4, "verison");

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
        case 1: // SPLIT_KEY_NAME
          return SPLIT_KEY_NAME;
        case 2: // LEVEL
          return LEVEL;
        case 3: // VALUE
          return VALUE;
        case 4: // VERISON
          return VERISON;
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
  private static final int __LEVEL_ISSET_ID = 0;
  private static final int __VERISON_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SPLIT_KEY_NAME, new org.apache.thrift.meta_data.FieldMetaData("splitKeyName", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.LEVEL, new org.apache.thrift.meta_data.FieldMetaData("level", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.VALUE, new org.apache.thrift.meta_data.FieldMetaData("value", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.VERISON, new org.apache.thrift.meta_data.FieldMetaData("verison", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SplitValue.class, metaDataMap);
  }

  public SplitValue() {
  }

  public SplitValue(
    String splitKeyName,
    int level,
    String value,
    long verison)
  {
    this();
    this.splitKeyName = splitKeyName;
    this.level = level;
    setLevelIsSet(true);
    this.value = value;
    this.verison = verison;
    setVerisonIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SplitValue(SplitValue other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetSplitKeyName()) {
      this.splitKeyName = other.splitKeyName;
    }
    this.level = other.level;
    if (other.isSetValue()) {
      this.value = other.value;
    }
    this.verison = other.verison;
  }

  public SplitValue deepCopy() {
    return new SplitValue(this);
  }

  @Override
  public void clear() {
    this.splitKeyName = null;
    setLevelIsSet(false);
    this.level = 0;
    this.value = null;
    setVerisonIsSet(false);
    this.verison = 0;
  }

  public String getSplitKeyName() {
    return this.splitKeyName;
  }

  public void setSplitKeyName(String splitKeyName) {
    this.splitKeyName = splitKeyName;
  }

  public void unsetSplitKeyName() {
    this.splitKeyName = null;
  }

  /** Returns true if field splitKeyName is set (has been assigned a value) and false otherwise */
  public boolean isSetSplitKeyName() {
    return this.splitKeyName != null;
  }

  public void setSplitKeyNameIsSet(boolean value) {
    if (!value) {
      this.splitKeyName = null;
    }
  }

  public int getLevel() {
    return this.level;
  }

  public void setLevel(int level) {
    this.level = level;
    setLevelIsSet(true);
  }

  public void unsetLevel() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __LEVEL_ISSET_ID);
  }

  /** Returns true if field level is set (has been assigned a value) and false otherwise */
  public boolean isSetLevel() {
    return EncodingUtils.testBit(__isset_bitfield, __LEVEL_ISSET_ID);
  }

  public void setLevelIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __LEVEL_ISSET_ID, value);
  }

  public String getValue() {
    return this.value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public void unsetValue() {
    this.value = null;
  }

  /** Returns true if field value is set (has been assigned a value) and false otherwise */
  public boolean isSetValue() {
    return this.value != null;
  }

  public void setValueIsSet(boolean value) {
    if (!value) {
      this.value = null;
    }
  }

  public long getVerison() {
    return this.verison;
  }

  public void setVerison(long verison) {
    this.verison = verison;
    setVerisonIsSet(true);
  }

  public void unsetVerison() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __VERISON_ISSET_ID);
  }

  /** Returns true if field verison is set (has been assigned a value) and false otherwise */
  public boolean isSetVerison() {
    return EncodingUtils.testBit(__isset_bitfield, __VERISON_ISSET_ID);
  }

  public void setVerisonIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __VERISON_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SPLIT_KEY_NAME:
      if (value == null) {
        unsetSplitKeyName();
      } else {
        setSplitKeyName((String)value);
      }
      break;

    case LEVEL:
      if (value == null) {
        unsetLevel();
      } else {
        setLevel((Integer)value);
      }
      break;

    case VALUE:
      if (value == null) {
        unsetValue();
      } else {
        setValue((String)value);
      }
      break;

    case VERISON:
      if (value == null) {
        unsetVerison();
      } else {
        setVerison((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SPLIT_KEY_NAME:
      return getSplitKeyName();

    case LEVEL:
      return Integer.valueOf(getLevel());

    case VALUE:
      return getValue();

    case VERISON:
      return Long.valueOf(getVerison());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SPLIT_KEY_NAME:
      return isSetSplitKeyName();
    case LEVEL:
      return isSetLevel();
    case VALUE:
      return isSetValue();
    case VERISON:
      return isSetVerison();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SplitValue)
      return this.equals((SplitValue)that);
    return false;
  }

  public boolean equals(SplitValue that) {
    if (that == null)
      return false;

    boolean this_present_splitKeyName = true && this.isSetSplitKeyName();
    boolean that_present_splitKeyName = true && that.isSetSplitKeyName();
    if (this_present_splitKeyName || that_present_splitKeyName) {
      if (!(this_present_splitKeyName && that_present_splitKeyName))
        return false;
      if (!this.splitKeyName.equals(that.splitKeyName))
        return false;
    }

    boolean this_present_level = true;
    boolean that_present_level = true;
    if (this_present_level || that_present_level) {
      if (!(this_present_level && that_present_level))
        return false;
      if (this.level != that.level)
        return false;
    }

    boolean this_present_value = true && this.isSetValue();
    boolean that_present_value = true && that.isSetValue();
    if (this_present_value || that_present_value) {
      if (!(this_present_value && that_present_value))
        return false;
      if (!this.value.equals(that.value))
        return false;
    }

    boolean this_present_verison = true;
    boolean that_present_verison = true;
    if (this_present_verison || that_present_verison) {
      if (!(this_present_verison && that_present_verison))
        return false;
      if (this.verison != that.verison)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_splitKeyName = true && (isSetSplitKeyName());
    builder.append(present_splitKeyName);
    if (present_splitKeyName)
      builder.append(splitKeyName);

    boolean present_level = true;
    builder.append(present_level);
    if (present_level)
      builder.append(level);

    boolean present_value = true && (isSetValue());
    builder.append(present_value);
    if (present_value)
      builder.append(value);

    boolean present_verison = true;
    builder.append(present_verison);
    if (present_verison)
      builder.append(verison);

    return builder.toHashCode();
  }

  public int compareTo(SplitValue other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    SplitValue typedOther = (SplitValue)other;

    lastComparison = Boolean.valueOf(isSetSplitKeyName()).compareTo(typedOther.isSetSplitKeyName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSplitKeyName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.splitKeyName, typedOther.splitKeyName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLevel()).compareTo(typedOther.isSetLevel());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLevel()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.level, typedOther.level);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetValue()).compareTo(typedOther.isSetValue());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValue()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.value, typedOther.value);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetVerison()).compareTo(typedOther.isSetVerison());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVerison()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.verison, typedOther.verison);
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
    StringBuilder sb = new StringBuilder("SplitValue(");
    boolean first = true;

    sb.append("splitKeyName:");
    if (this.splitKeyName == null) {
      sb.append("null");
    } else {
      sb.append(this.splitKeyName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("level:");
    sb.append(this.level);
    first = false;
    if (!first) sb.append(", ");
    sb.append("value:");
    if (this.value == null) {
      sb.append("null");
    } else {
      sb.append(this.value);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("verison:");
    sb.append(this.verison);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class SplitValueStandardSchemeFactory implements SchemeFactory {
    public SplitValueStandardScheme getScheme() {
      return new SplitValueStandardScheme();
    }
  }

  private static class SplitValueStandardScheme extends StandardScheme<SplitValue> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SplitValue struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SPLIT_KEY_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.splitKeyName = iprot.readString();
              struct.setSplitKeyNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // LEVEL
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.level = iprot.readI32();
              struct.setLevelIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // VALUE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.value = iprot.readString();
              struct.setValueIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // VERISON
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.verison = iprot.readI64();
              struct.setVerisonIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, SplitValue struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.splitKeyName != null) {
        oprot.writeFieldBegin(SPLIT_KEY_NAME_FIELD_DESC);
        oprot.writeString(struct.splitKeyName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(LEVEL_FIELD_DESC);
      oprot.writeI32(struct.level);
      oprot.writeFieldEnd();
      if (struct.value != null) {
        oprot.writeFieldBegin(VALUE_FIELD_DESC);
        oprot.writeString(struct.value);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(VERISON_FIELD_DESC);
      oprot.writeI64(struct.verison);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SplitValueTupleSchemeFactory implements SchemeFactory {
    public SplitValueTupleScheme getScheme() {
      return new SplitValueTupleScheme();
    }
  }

  private static class SplitValueTupleScheme extends TupleScheme<SplitValue> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SplitValue struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetSplitKeyName()) {
        optionals.set(0);
      }
      if (struct.isSetLevel()) {
        optionals.set(1);
      }
      if (struct.isSetValue()) {
        optionals.set(2);
      }
      if (struct.isSetVerison()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetSplitKeyName()) {
        oprot.writeString(struct.splitKeyName);
      }
      if (struct.isSetLevel()) {
        oprot.writeI32(struct.level);
      }
      if (struct.isSetValue()) {
        oprot.writeString(struct.value);
      }
      if (struct.isSetVerison()) {
        oprot.writeI64(struct.verison);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SplitValue struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.splitKeyName = iprot.readString();
        struct.setSplitKeyNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.level = iprot.readI32();
        struct.setLevelIsSet(true);
      }
      if (incoming.get(2)) {
        struct.value = iprot.readString();
        struct.setValueIsSet(true);
      }
      if (incoming.get(3)) {
        struct.verison = iprot.readI64();
        struct.setVerisonIsSet(true);
      }
    }
  }

}

