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

public class EquipRoom implements org.apache.thrift.TBase<EquipRoom, EquipRoom._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("EquipRoom");

  private static final org.apache.thrift.protocol.TField EQ_ROOM_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("eqRoomName", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift.protocol.TField("status", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField COMMENT_FIELD_DESC = new org.apache.thrift.protocol.TField("comment", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField GEOLOCATION_FIELD_DESC = new org.apache.thrift.protocol.TField("geolocation", org.apache.thrift.protocol.TType.STRUCT, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new EquipRoomStandardSchemeFactory());
    schemes.put(TupleScheme.class, new EquipRoomTupleSchemeFactory());
  }

  private String eqRoomName; // required
  private int status; // required
  private String comment; // optional
  private GeoLocation geolocation; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    EQ_ROOM_NAME((short)1, "eqRoomName"),
    STATUS((short)2, "status"),
    COMMENT((short)3, "comment"),
    GEOLOCATION((short)4, "geolocation");

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
        case 1: // EQ_ROOM_NAME
          return EQ_ROOM_NAME;
        case 2: // STATUS
          return STATUS;
        case 3: // COMMENT
          return COMMENT;
        case 4: // GEOLOCATION
          return GEOLOCATION;
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
  private static final int __STATUS_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private _Fields optionals[] = {_Fields.COMMENT,_Fields.GEOLOCATION};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.EQ_ROOM_NAME, new org.apache.thrift.meta_data.FieldMetaData("eqRoomName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STATUS, new org.apache.thrift.meta_data.FieldMetaData("status", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.COMMENT, new org.apache.thrift.meta_data.FieldMetaData("comment", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.GEOLOCATION, new org.apache.thrift.meta_data.FieldMetaData("geolocation", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, GeoLocation.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(EquipRoom.class, metaDataMap);
  }

  public EquipRoom() {
  }

  public EquipRoom(
    String eqRoomName,
    int status)
  {
    this();
    this.eqRoomName = eqRoomName;
    this.status = status;
    setStatusIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public EquipRoom(EquipRoom other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetEqRoomName()) {
      this.eqRoomName = other.eqRoomName;
    }
    this.status = other.status;
    if (other.isSetComment()) {
      this.comment = other.comment;
    }
    if (other.isSetGeolocation()) {
      this.geolocation = new GeoLocation(other.geolocation);
    }
  }

  public EquipRoom deepCopy() {
    return new EquipRoom(this);
  }

  @Override
  public void clear() {
    this.eqRoomName = null;
    setStatusIsSet(false);
    this.status = 0;
    this.comment = null;
    this.geolocation = null;
  }

  public String getEqRoomName() {
    return this.eqRoomName;
  }

  public void setEqRoomName(String eqRoomName) {
    this.eqRoomName = eqRoomName;
  }

  public void unsetEqRoomName() {
    this.eqRoomName = null;
  }

  /** Returns true if field eqRoomName is set (has been assigned a value) and false otherwise */
  public boolean isSetEqRoomName() {
    return this.eqRoomName != null;
  }

  public void setEqRoomNameIsSet(boolean value) {
    if (!value) {
      this.eqRoomName = null;
    }
  }

  public int getStatus() {
    return this.status;
  }

  public void setStatus(int status) {
    this.status = status;
    setStatusIsSet(true);
  }

  public void unsetStatus() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __STATUS_ISSET_ID);
  }

  /** Returns true if field status is set (has been assigned a value) and false otherwise */
  public boolean isSetStatus() {
    return EncodingUtils.testBit(__isset_bitfield, __STATUS_ISSET_ID);
  }

  public void setStatusIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __STATUS_ISSET_ID, value);
  }

  public String getComment() {
    return this.comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public void unsetComment() {
    this.comment = null;
  }

  /** Returns true if field comment is set (has been assigned a value) and false otherwise */
  public boolean isSetComment() {
    return this.comment != null;
  }

  public void setCommentIsSet(boolean value) {
    if (!value) {
      this.comment = null;
    }
  }

  public GeoLocation getGeolocation() {
    return this.geolocation;
  }

  public void setGeolocation(GeoLocation geolocation) {
    this.geolocation = geolocation;
  }

  public void unsetGeolocation() {
    this.geolocation = null;
  }

  /** Returns true if field geolocation is set (has been assigned a value) and false otherwise */
  public boolean isSetGeolocation() {
    return this.geolocation != null;
  }

  public void setGeolocationIsSet(boolean value) {
    if (!value) {
      this.geolocation = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case EQ_ROOM_NAME:
      if (value == null) {
        unsetEqRoomName();
      } else {
        setEqRoomName((String)value);
      }
      break;

    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((Integer)value);
      }
      break;

    case COMMENT:
      if (value == null) {
        unsetComment();
      } else {
        setComment((String)value);
      }
      break;

    case GEOLOCATION:
      if (value == null) {
        unsetGeolocation();
      } else {
        setGeolocation((GeoLocation)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case EQ_ROOM_NAME:
      return getEqRoomName();

    case STATUS:
      return Integer.valueOf(getStatus());

    case COMMENT:
      return getComment();

    case GEOLOCATION:
      return getGeolocation();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case EQ_ROOM_NAME:
      return isSetEqRoomName();
    case STATUS:
      return isSetStatus();
    case COMMENT:
      return isSetComment();
    case GEOLOCATION:
      return isSetGeolocation();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof EquipRoom)
      return this.equals((EquipRoom)that);
    return false;
  }

  public boolean equals(EquipRoom that) {
    if (that == null)
      return false;

    boolean this_present_eqRoomName = true && this.isSetEqRoomName();
    boolean that_present_eqRoomName = true && that.isSetEqRoomName();
    if (this_present_eqRoomName || that_present_eqRoomName) {
      if (!(this_present_eqRoomName && that_present_eqRoomName))
        return false;
      if (!this.eqRoomName.equals(that.eqRoomName))
        return false;
    }

    boolean this_present_status = true;
    boolean that_present_status = true;
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (this.status != that.status)
        return false;
    }

    boolean this_present_comment = true && this.isSetComment();
    boolean that_present_comment = true && that.isSetComment();
    if (this_present_comment || that_present_comment) {
      if (!(this_present_comment && that_present_comment))
        return false;
      if (!this.comment.equals(that.comment))
        return false;
    }

    boolean this_present_geolocation = true && this.isSetGeolocation();
    boolean that_present_geolocation = true && that.isSetGeolocation();
    if (this_present_geolocation || that_present_geolocation) {
      if (!(this_present_geolocation && that_present_geolocation))
        return false;
      if (!this.geolocation.equals(that.geolocation))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_eqRoomName = true && (isSetEqRoomName());
    builder.append(present_eqRoomName);
    if (present_eqRoomName)
      builder.append(eqRoomName);

    boolean present_status = true;
    builder.append(present_status);
    if (present_status)
      builder.append(status);

    boolean present_comment = true && (isSetComment());
    builder.append(present_comment);
    if (present_comment)
      builder.append(comment);

    boolean present_geolocation = true && (isSetGeolocation());
    builder.append(present_geolocation);
    if (present_geolocation)
      builder.append(geolocation);

    return builder.toHashCode();
  }

  public int compareTo(EquipRoom other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    EquipRoom typedOther = (EquipRoom)other;

    lastComparison = Boolean.valueOf(isSetEqRoomName()).compareTo(typedOther.isSetEqRoomName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEqRoomName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.eqRoomName, typedOther.eqRoomName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStatus()).compareTo(typedOther.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatus()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status, typedOther.status);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetComment()).compareTo(typedOther.isSetComment());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetComment()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.comment, typedOther.comment);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetGeolocation()).compareTo(typedOther.isSetGeolocation());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetGeolocation()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.geolocation, typedOther.geolocation);
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
    StringBuilder sb = new StringBuilder("EquipRoom(");
    boolean first = true;

    sb.append("eqRoomName:");
    if (this.eqRoomName == null) {
      sb.append("null");
    } else {
      sb.append(this.eqRoomName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("status:");
    sb.append(this.status);
    first = false;
    if (isSetComment()) {
      if (!first) sb.append(", ");
      sb.append("comment:");
      if (this.comment == null) {
        sb.append("null");
      } else {
        sb.append(this.comment);
      }
      first = false;
    }
    if (isSetGeolocation()) {
      if (!first) sb.append(", ");
      sb.append("geolocation:");
      if (this.geolocation == null) {
        sb.append("null");
      } else {
        sb.append(this.geolocation);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetEqRoomName()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'eqRoomName' is unset! Struct:" + toString());
    }

    if (!isSetStatus()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'status' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (geolocation != null) {
      geolocation.validate();
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class EquipRoomStandardSchemeFactory implements SchemeFactory {
    public EquipRoomStandardScheme getScheme() {
      return new EquipRoomStandardScheme();
    }
  }

  private static class EquipRoomStandardScheme extends StandardScheme<EquipRoom> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, EquipRoom struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // EQ_ROOM_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.eqRoomName = iprot.readString();
              struct.setEqRoomNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STATUS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.status = iprot.readI32();
              struct.setStatusIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // COMMENT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.comment = iprot.readString();
              struct.setCommentIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // GEOLOCATION
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.geolocation = new GeoLocation();
              struct.geolocation.read(iprot);
              struct.setGeolocationIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, EquipRoom struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.eqRoomName != null) {
        oprot.writeFieldBegin(EQ_ROOM_NAME_FIELD_DESC);
        oprot.writeString(struct.eqRoomName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(STATUS_FIELD_DESC);
      oprot.writeI32(struct.status);
      oprot.writeFieldEnd();
      if (struct.comment != null) {
        if (struct.isSetComment()) {
          oprot.writeFieldBegin(COMMENT_FIELD_DESC);
          oprot.writeString(struct.comment);
          oprot.writeFieldEnd();
        }
      }
      if (struct.geolocation != null) {
        if (struct.isSetGeolocation()) {
          oprot.writeFieldBegin(GEOLOCATION_FIELD_DESC);
          struct.geolocation.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class EquipRoomTupleSchemeFactory implements SchemeFactory {
    public EquipRoomTupleScheme getScheme() {
      return new EquipRoomTupleScheme();
    }
  }

  private static class EquipRoomTupleScheme extends TupleScheme<EquipRoom> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, EquipRoom struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.eqRoomName);
      oprot.writeI32(struct.status);
      BitSet optionals = new BitSet();
      if (struct.isSetComment()) {
        optionals.set(0);
      }
      if (struct.isSetGeolocation()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetComment()) {
        oprot.writeString(struct.comment);
      }
      if (struct.isSetGeolocation()) {
        struct.geolocation.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, EquipRoom struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.eqRoomName = iprot.readString();
      struct.setEqRoomNameIsSet(true);
      struct.status = iprot.readI32();
      struct.setStatusIsSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.comment = iprot.readString();
        struct.setCommentIsSet(true);
      }
      if (incoming.get(1)) {
        struct.geolocation = new GeoLocation();
        struct.geolocation.read(iprot);
        struct.setGeolocationIsSet(true);
      }
    }
  }

}

