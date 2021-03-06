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

public class GeoLocation implements org.apache.thrift.TBase<GeoLocation, GeoLocation._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("GeoLocation");

  private static final org.apache.thrift.protocol.TField GEO_LOC_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("geoLocName", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField NATION_FIELD_DESC = new org.apache.thrift.protocol.TField("nation", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField PROVINCE_FIELD_DESC = new org.apache.thrift.protocol.TField("province", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField CITY_FIELD_DESC = new org.apache.thrift.protocol.TField("city", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField DIST_FIELD_DESC = new org.apache.thrift.protocol.TField("dist", org.apache.thrift.protocol.TType.STRING, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new GeoLocationStandardSchemeFactory());
    schemes.put(TupleScheme.class, new GeoLocationTupleSchemeFactory());
  }

  private String geoLocName; // required
  private String nation; // required
  private String province; // required
  private String city; // required
  private String dist; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    GEO_LOC_NAME((short)1, "geoLocName"),
    NATION((short)2, "nation"),
    PROVINCE((short)3, "province"),
    CITY((short)4, "city"),
    DIST((short)5, "dist");

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
        case 1: // GEO_LOC_NAME
          return GEO_LOC_NAME;
        case 2: // NATION
          return NATION;
        case 3: // PROVINCE
          return PROVINCE;
        case 4: // CITY
          return CITY;
        case 5: // DIST
          return DIST;
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
    tmpMap.put(_Fields.GEO_LOC_NAME, new org.apache.thrift.meta_data.FieldMetaData("geoLocName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.NATION, new org.apache.thrift.meta_data.FieldMetaData("nation", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PROVINCE, new org.apache.thrift.meta_data.FieldMetaData("province", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CITY, new org.apache.thrift.meta_data.FieldMetaData("city", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.DIST, new org.apache.thrift.meta_data.FieldMetaData("dist", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(GeoLocation.class, metaDataMap);
  }

  public GeoLocation() {
  }

  public GeoLocation(
    String geoLocName,
    String nation,
    String province,
    String city,
    String dist)
  {
    this();
    this.geoLocName = geoLocName;
    this.nation = nation;
    this.province = province;
    this.city = city;
    this.dist = dist;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GeoLocation(GeoLocation other) {
    if (other.isSetGeoLocName()) {
      this.geoLocName = other.geoLocName;
    }
    if (other.isSetNation()) {
      this.nation = other.nation;
    }
    if (other.isSetProvince()) {
      this.province = other.province;
    }
    if (other.isSetCity()) {
      this.city = other.city;
    }
    if (other.isSetDist()) {
      this.dist = other.dist;
    }
  }

  public GeoLocation deepCopy() {
    return new GeoLocation(this);
  }

  @Override
  public void clear() {
    this.geoLocName = null;
    this.nation = null;
    this.province = null;
    this.city = null;
    this.dist = null;
  }

  public String getGeoLocName() {
    return this.geoLocName;
  }

  public void setGeoLocName(String geoLocName) {
    this.geoLocName = geoLocName;
  }

  public void unsetGeoLocName() {
    this.geoLocName = null;
  }

  /** Returns true if field geoLocName is set (has been assigned a value) and false otherwise */
  public boolean isSetGeoLocName() {
    return this.geoLocName != null;
  }

  public void setGeoLocNameIsSet(boolean value) {
    if (!value) {
      this.geoLocName = null;
    }
  }

  public String getNation() {
    return this.nation;
  }

  public void setNation(String nation) {
    this.nation = nation;
  }

  public void unsetNation() {
    this.nation = null;
  }

  /** Returns true if field nation is set (has been assigned a value) and false otherwise */
  public boolean isSetNation() {
    return this.nation != null;
  }

  public void setNationIsSet(boolean value) {
    if (!value) {
      this.nation = null;
    }
  }

  public String getProvince() {
    return this.province;
  }

  public void setProvince(String province) {
    this.province = province;
  }

  public void unsetProvince() {
    this.province = null;
  }

  /** Returns true if field province is set (has been assigned a value) and false otherwise */
  public boolean isSetProvince() {
    return this.province != null;
  }

  public void setProvinceIsSet(boolean value) {
    if (!value) {
      this.province = null;
    }
  }

  public String getCity() {
    return this.city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public void unsetCity() {
    this.city = null;
  }

  /** Returns true if field city is set (has been assigned a value) and false otherwise */
  public boolean isSetCity() {
    return this.city != null;
  }

  public void setCityIsSet(boolean value) {
    if (!value) {
      this.city = null;
    }
  }

  public String getDist() {
    return this.dist;
  }

  public void setDist(String dist) {
    this.dist = dist;
  }

  public void unsetDist() {
    this.dist = null;
  }

  /** Returns true if field dist is set (has been assigned a value) and false otherwise */
  public boolean isSetDist() {
    return this.dist != null;
  }

  public void setDistIsSet(boolean value) {
    if (!value) {
      this.dist = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case GEO_LOC_NAME:
      if (value == null) {
        unsetGeoLocName();
      } else {
        setGeoLocName((String)value);
      }
      break;

    case NATION:
      if (value == null) {
        unsetNation();
      } else {
        setNation((String)value);
      }
      break;

    case PROVINCE:
      if (value == null) {
        unsetProvince();
      } else {
        setProvince((String)value);
      }
      break;

    case CITY:
      if (value == null) {
        unsetCity();
      } else {
        setCity((String)value);
      }
      break;

    case DIST:
      if (value == null) {
        unsetDist();
      } else {
        setDist((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case GEO_LOC_NAME:
      return getGeoLocName();

    case NATION:
      return getNation();

    case PROVINCE:
      return getProvince();

    case CITY:
      return getCity();

    case DIST:
      return getDist();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case GEO_LOC_NAME:
      return isSetGeoLocName();
    case NATION:
      return isSetNation();
    case PROVINCE:
      return isSetProvince();
    case CITY:
      return isSetCity();
    case DIST:
      return isSetDist();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof GeoLocation)
      return this.equals((GeoLocation)that);
    return false;
  }

  public boolean equals(GeoLocation that) {
    if (that == null)
      return false;

    boolean this_present_geoLocName = true && this.isSetGeoLocName();
    boolean that_present_geoLocName = true && that.isSetGeoLocName();
    if (this_present_geoLocName || that_present_geoLocName) {
      if (!(this_present_geoLocName && that_present_geoLocName))
        return false;
      if (!this.geoLocName.equals(that.geoLocName))
        return false;
    }

    boolean this_present_nation = true && this.isSetNation();
    boolean that_present_nation = true && that.isSetNation();
    if (this_present_nation || that_present_nation) {
      if (!(this_present_nation && that_present_nation))
        return false;
      if (!this.nation.equals(that.nation))
        return false;
    }

    boolean this_present_province = true && this.isSetProvince();
    boolean that_present_province = true && that.isSetProvince();
    if (this_present_province || that_present_province) {
      if (!(this_present_province && that_present_province))
        return false;
      if (!this.province.equals(that.province))
        return false;
    }

    boolean this_present_city = true && this.isSetCity();
    boolean that_present_city = true && that.isSetCity();
    if (this_present_city || that_present_city) {
      if (!(this_present_city && that_present_city))
        return false;
      if (!this.city.equals(that.city))
        return false;
    }

    boolean this_present_dist = true && this.isSetDist();
    boolean that_present_dist = true && that.isSetDist();
    if (this_present_dist || that_present_dist) {
      if (!(this_present_dist && that_present_dist))
        return false;
      if (!this.dist.equals(that.dist))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_geoLocName = true && (isSetGeoLocName());
    builder.append(present_geoLocName);
    if (present_geoLocName)
      builder.append(geoLocName);

    boolean present_nation = true && (isSetNation());
    builder.append(present_nation);
    if (present_nation)
      builder.append(nation);

    boolean present_province = true && (isSetProvince());
    builder.append(present_province);
    if (present_province)
      builder.append(province);

    boolean present_city = true && (isSetCity());
    builder.append(present_city);
    if (present_city)
      builder.append(city);

    boolean present_dist = true && (isSetDist());
    builder.append(present_dist);
    if (present_dist)
      builder.append(dist);

    return builder.toHashCode();
  }

  public int compareTo(GeoLocation other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    GeoLocation typedOther = (GeoLocation)other;

    lastComparison = Boolean.valueOf(isSetGeoLocName()).compareTo(typedOther.isSetGeoLocName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetGeoLocName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.geoLocName, typedOther.geoLocName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNation()).compareTo(typedOther.isSetNation());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNation()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.nation, typedOther.nation);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetProvince()).compareTo(typedOther.isSetProvince());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetProvince()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.province, typedOther.province);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCity()).compareTo(typedOther.isSetCity());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCity()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.city, typedOther.city);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDist()).compareTo(typedOther.isSetDist());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDist()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dist, typedOther.dist);
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
    StringBuilder sb = new StringBuilder("GeoLocation(");
    boolean first = true;

    sb.append("geoLocName:");
    if (this.geoLocName == null) {
      sb.append("null");
    } else {
      sb.append(this.geoLocName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("nation:");
    if (this.nation == null) {
      sb.append("null");
    } else {
      sb.append(this.nation);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("province:");
    if (this.province == null) {
      sb.append("null");
    } else {
      sb.append(this.province);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("city:");
    if (this.city == null) {
      sb.append("null");
    } else {
      sb.append(this.city);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("dist:");
    if (this.dist == null) {
      sb.append("null");
    } else {
      sb.append(this.dist);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetGeoLocName()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'geoLocName' is unset! Struct:" + toString());
    }

    if (!isSetNation()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'nation' is unset! Struct:" + toString());
    }

    if (!isSetProvince()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'province' is unset! Struct:" + toString());
    }

    if (!isSetCity()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'city' is unset! Struct:" + toString());
    }

    if (!isSetDist()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'dist' is unset! Struct:" + toString());
    }

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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class GeoLocationStandardSchemeFactory implements SchemeFactory {
    public GeoLocationStandardScheme getScheme() {
      return new GeoLocationStandardScheme();
    }
  }

  private static class GeoLocationStandardScheme extends StandardScheme<GeoLocation> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, GeoLocation struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // GEO_LOC_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.geoLocName = iprot.readString();
              struct.setGeoLocNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // NATION
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.nation = iprot.readString();
              struct.setNationIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // PROVINCE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.province = iprot.readString();
              struct.setProvinceIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // CITY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.city = iprot.readString();
              struct.setCityIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // DIST
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.dist = iprot.readString();
              struct.setDistIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, GeoLocation struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.geoLocName != null) {
        oprot.writeFieldBegin(GEO_LOC_NAME_FIELD_DESC);
        oprot.writeString(struct.geoLocName);
        oprot.writeFieldEnd();
      }
      if (struct.nation != null) {
        oprot.writeFieldBegin(NATION_FIELD_DESC);
        oprot.writeString(struct.nation);
        oprot.writeFieldEnd();
      }
      if (struct.province != null) {
        oprot.writeFieldBegin(PROVINCE_FIELD_DESC);
        oprot.writeString(struct.province);
        oprot.writeFieldEnd();
      }
      if (struct.city != null) {
        oprot.writeFieldBegin(CITY_FIELD_DESC);
        oprot.writeString(struct.city);
        oprot.writeFieldEnd();
      }
      if (struct.dist != null) {
        oprot.writeFieldBegin(DIST_FIELD_DESC);
        oprot.writeString(struct.dist);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GeoLocationTupleSchemeFactory implements SchemeFactory {
    public GeoLocationTupleScheme getScheme() {
      return new GeoLocationTupleScheme();
    }
  }

  private static class GeoLocationTupleScheme extends TupleScheme<GeoLocation> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, GeoLocation struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.geoLocName);
      oprot.writeString(struct.nation);
      oprot.writeString(struct.province);
      oprot.writeString(struct.city);
      oprot.writeString(struct.dist);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, GeoLocation struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.geoLocName = iprot.readString();
      struct.setGeoLocNameIsSet(true);
      struct.nation = iprot.readString();
      struct.setNationIsSet(true);
      struct.province = iprot.readString();
      struct.setProvinceIsSet(true);
      struct.city = iprot.readString();
      struct.setCityIsSet(true);
      struct.dist = iprot.readString();
      struct.setDistIsSet(true);
    }
  }

}

