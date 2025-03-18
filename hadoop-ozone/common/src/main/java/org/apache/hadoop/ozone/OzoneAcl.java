/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.protobuf.ByteString;
import com.google.protobuf.Proto2Utils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.ratis.util.MemoizedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OzoneACL classes define bucket ACLs used in OZONE.
 *
 * ACLs in Ozone follow this pattern.
 * <ul>
 * <li>user:name:rw
 * <li>group:name:rw
 * <li>world::rw
 * </ul>
 */
public interface OzoneAcl {

  /**
   * Link bucket default acl defined [world::rw]
   * which is similar to Linux POSIX symbolic.
   */
  OzoneAcl LINK_BUCKET_DEFAULT_ACL =
      OzoneAcl.of(IAccessAuthorizer.ACLIdentityType.WORLD, "", ACCESS, READ, WRITE);

  // instance methods
  String getName();
  AclScope getAclScope();
  ACLIdentityType getType();
  @JsonIgnore int getBits();

  // factories
  static OzoneAcl of(ACLIdentityType type, String name, AclScope scope, ACLType... acls) {
    return new Impl(type, name, scope, Util.toInt(acls));
  }

  static OzoneAcl of(ACLIdentityType type, String name, AclScope scope, EnumSet<ACLType> acls) {
    return new Impl(type, name, scope, Util.toInt(acls));
  }

  static OzoneAcl of(ACLIdentityType type, String name, AclScope scope, int acls) {
    return new Impl(type, name, scope, acls);
  }

  /**
   * Parses an ACL string and returns the ACL object. If acl scope is not
   * passed in input string then scope is set to ACCESS.
   *
   * @param acl - Acl String , Ex. user:anu:rw
   *
   * @return - Ozone ACLs
   */
  static OzoneAcl parseAcl(String acl)
      throws IllegalArgumentException {
    if ((acl == null) || acl.isEmpty()) {
      throw new IllegalArgumentException("ACLs cannot be null or empty");
    }
    String[] parts = acl.trim().split(":");
    if (parts.length < 3) {
      throw new IllegalArgumentException("ACLs are not in expected format");
    }

    ACLIdentityType aclType = ACLIdentityType.valueOf(parts[0].toUpperCase());

    String bits = parts[2];

    // Default acl scope is ACCESS.
    AclScope aclScope = AclScope.ACCESS;

    // Check if acl string contains scope info.
    if (parts[2].matches(Util.ACL_SCOPE_REGEX)) {
      int indexOfOpenBracket = parts[2].indexOf("[");
      bits = parts[2].substring(0, indexOfOpenBracket);
      aclScope = AclScope.valueOf(parts[2].substring(indexOfOpenBracket + 1,
          parts[2].indexOf("]")));
    }

    EnumSet<ACLType> acls = EnumSet.noneOf(ACLType.class);
    for (char ch : bits.toCharArray()) {
      acls.add(ACLType.getACLRight(String.valueOf(ch)));
    }

    // TODO : Support sanitation of these user names by calling into
    // userAuth Interface.
    return OzoneAcl.of(aclType, parts[1], aclScope, acls);
  }

  /**
   * Parses an ACL string and returns the ACL object.
   *
   * @param acls - Acl String , Ex. user:anu:rw
   *
   * @return - Ozone ACLs
   */
  static List<OzoneAcl> parseAcls(String acls)
      throws IllegalArgumentException {
    if ((acls == null) || acls.isEmpty()) {
      throw new IllegalArgumentException("ACLs cannot be null or empty");
    }
    String[] parts = acls.trim().split(",");
    if (parts.length < 1) {
      throw new IllegalArgumentException("ACLs are not in expected format");
    }
    List<OzoneAcl> ozAcls = new ArrayList<>();

    for (String acl:parts) {
      ozAcls.add(parseAcl(acl));
    }
    return ozAcls;
  }

  default OzoneAcl withScope(final AclScope scope) {
    return scope == getAclScope() ? this
        : OzoneAcl.of(getType(), getName(), scope, getBits());
  }

  // proto conversion

  default OzoneAclInfo toProto() {
    return OzoneAclInfo.newBuilder()
        .setName(getName())
        .setType(OzoneAclType.valueOf(getType().name()))
        .setAclScope(OzoneAclScope.valueOf(getAclScope().name()))
        .setRights(getAclByteString()).build();
  }

  static OzoneAclInfo toProtobuf(OzoneAcl acl) {
    return acl.toProto();
  }

  static OzoneAcl fromProtobuf(OzoneAclInfo proto) {
    return new ProtoWrapper(proto);
  }

  @JsonIgnore
  default boolean isEmpty() {
    return getBits() == 0;
  }

  default boolean isSet(ACLType acl) {
    return (getBits() & Util.toInt(acl)) != 0;
  }

  default boolean checkAccess(ACLType acl) {
    return (isSet(acl) || isSet(ALL)) && !isSet(NONE);
  }

  default OzoneAcl add(OzoneAcl other) {
    return apply(bits -> bits | other.getBits());
  }

  default OzoneAcl remove(OzoneAcl other) {
    return apply(bits -> bits & ~other.getBits());
  }

  default OzoneAcl apply(IntFunction<Integer> op) {
    int applied = op.apply(getBits());
    return applied == getBits()
        ? this
        : OzoneAcl.of(getType(), getName(), getAclScope(), applied);
  }

  @JsonIgnore
  default ByteString getAclByteString() {
    // only first 9 bits are used currently
    final int bits = getBits();
    final byte first = (byte) bits;
    final byte second = (byte) (bits >>> 8);
    final byte[] bytes = second != 0 ? new byte[]{first, second} : new byte[]{first};
    return Proto2Utils.unsafeByteString(bytes);
  }

  @JsonIgnore
  default List<String> getAclStringList() {
    return Util.getAclList(getBits(), ACLType::name);
  }

  default List<ACLType> getAclList() {
    return Util.getAclList(getBits(), Function.identity());
  }

  /**
   * Scope of ozone acl.
   * */
  enum AclScope {
    ACCESS,
    DEFAULT,
  }

  @Immutable
  class Impl implements OzoneAcl {

    private final ACLIdentityType type;
    private final String name;
    @JsonIgnore
    private final int aclBits;
    private final AclScope aclScope;

    @JsonIgnore
    private final Supplier<String> toStringMethod;
    @JsonIgnore
    private final Supplier<Integer> hashCodeMethod;

    private Impl(ACLIdentityType type, String name, AclScope scope, int acls) {
      this.name = Util.validateNameAndType(type, name);
      this.type = type;
      this.aclScope = scope;
      this.aclBits = acls;
      this.toStringMethod = MemoizedSupplier.valueOf(() -> Util.asString(this));
      this.hashCodeMethod = MemoizedSupplier.valueOf(() -> Util.getHash(this));
    }

    @Override
    public String toString() {
      return toStringMethod.get();
    }

    @Override
    public int hashCode() {
      return hashCodeMethod.get();
    }

    @Override
    public String getName() {
      return name;
    }

    @JsonIgnore
    @Override
    public int getBits() {
      return aclBits;
    }

    @Override
    public AclScope getAclScope() {
      return aclScope;
    }

    @Override
    public ACLIdentityType getType() {
      return type;
    }

    @Override
    public boolean equals(Object obj) {
      return Util.equals(this, obj);
    }
  }

  @Immutable
  class ProtoWrapper implements OzoneAcl {

    private final OzoneAclInfo proto;
    private final MemoizedSupplier<OzoneAcl> converted;
    @JsonIgnore
    private final Supplier<String> toStringMethod;
    @JsonIgnore
    private final Supplier<Integer> hashCodeMethod;

    ProtoWrapper(OzoneAclInfo proto) {
      this.proto = proto;
      this.converted = MemoizedSupplier.valueOf(() -> Util.fromProto(proto));
      this.toStringMethod = MemoizedSupplier.valueOf(() -> Util.asString(this));
      this.hashCodeMethod = MemoizedSupplier.valueOf(() -> Util.getHash(this));
    }

    @Override
    public String getName() {
      return proto.getName();
    }

    @Override
    public ACLIdentityType getType() {
      return converted.get().getType();
    }

    @Override
    public AclScope getAclScope() {
      return converted.get().getAclScope();
    }

    @Override
    public OzoneAcl add(OzoneAcl other) {
      return converted.get().add(other);
    }

    @Override
    public OzoneAcl remove(OzoneAcl other) {
      return converted.get().remove(other);
    }

    @Override
    public int getBits() {
      return converted.get().getBits();
    }

    @Override
    public OzoneAclInfo toProto() {
      return proto;
    }

    @Override
    public boolean equals(Object obj) {
      return Util.equals(this, obj);
    }

    @Override
    public String toString() {
      return toStringMethod.get();
    }

    @Override
    public int hashCode() {
      return hashCodeMethod.get();
    }

    @Override
    public boolean isEmpty() {
      return proto.getRights().isEmpty();
    }
  }

  /** Private items. */
  final class Util {
    private static final Logger LOG = LoggerFactory.getLogger(OzoneAcl.class);

    // TODO use Pattern
    private static final String ACL_SCOPE_REGEX = ".*\\[(ACCESS|DEFAULT)\\]";

    private static int toInt(int aclTypeOrdinal) {
      return 1 << aclTypeOrdinal;
    }

    private static int toInt(ACLType acl) {
      return toInt(acl.ordinal());
    }

    private static int toInt(ACLType[] acls) {
      if (acls == null) {
        return 0;
      }
      int value = 0;
      for (ACLType acl : acls) {
        value |= toInt(acl);
      }
      return value;
    }

    private static int toInt(Iterable<ACLType> acls) {
      if (acls == null) {
        return 0;
      }
      int value = 0;
      for (ACLType acl : acls) {
        value |= toInt(acl);
      }
      return value;
    }

    private static String validateNameAndType(ACLIdentityType type, String name) {
      Objects.requireNonNull(type);

      if (type == ACLIdentityType.WORLD || type == ACLIdentityType.ANONYMOUS) {
        if (!name.equals(ACLIdentityType.WORLD.name()) &&
            !name.equals(ACLIdentityType.ANONYMOUS.name()) &&
            !name.isEmpty()) {
          throw new IllegalArgumentException("Expected name " + type.name() + ", but was: " + name);
        }
        // For type WORLD and ANONYMOUS we allow only one acl to be set.
        return type.name();
      }

      if (((type == ACLIdentityType.USER) || (type == ACLIdentityType.GROUP))
          && (name.isEmpty())) {
        throw new IllegalArgumentException(type + " name is required");
      }

      return name;
    }

    private static <T> List<T> getAclList(int aclBits, Function<ACLType, T> converter) {
      if (aclBits == 0) {
        return Collections.emptyList();
      }
      final List<T> toReturn = new ArrayList<>(Integer.bitCount(aclBits));
      for (int i = 0; i < ACLType.values().length; i++) {
        if ((toInt(i) & aclBits) != 0) {
          toReturn.add(converter.apply(ACLType.values()[i]));
        }
      }
      return Collections.unmodifiableList(toReturn);
    }

    private static OzoneAcl fromProto(OzoneAclInfo protoAcl) {
      final byte[] bytes = protoAcl.getRights().toByteArray();
      if (bytes.length > 4) {
        throw new AssertionError("Expected at most 4 bytes but got " + bytes.length);
      }
      int aclRights = 0;
      for (int i = 0; i < bytes.length; i++) {
        aclRights |= (bytes[i] & 0xff) << (i * 8);
      }
      OzoneAcl acl = OzoneAcl.of(ACLIdentityType.valueOf(protoAcl.getType().name()), protoAcl.getName(),
          AclScope.valueOf(protoAcl.getAclScope().name()), aclRights);
      LOG.info("ZZZ fromProto {}", acl);
      return acl;
    }

    private static boolean equals(final OzoneAcl acl, final Object obj) {
      if (acl == obj) {
        return true;
      }
      if (!(obj instanceof OzoneAcl)) {
        return false;
      }
      OzoneAcl other = (OzoneAcl) obj;
      return Objects.equals(acl.getName(), other.getName())
          && Objects.equals(acl.getType(), other.getType())
          && Objects.equals(acl.getAclScope(), other.getAclScope())
          && Objects.equals(acl.getBits(), other.getBits());
    }

    private static int getHash(OzoneAcl acl) {
      return Objects.hash(acl.getName(),
          BitSet.valueOf(acl.getAclByteString().asReadOnlyByteBuffer()),
          acl.getType().toString(),
          acl.getAclScope());
    }

    private static String asString(OzoneAcl acl) {
      return acl.getType() + ":" + acl.getName() + ":"
          + ACLType.getACLString(BitSet.valueOf(acl.getAclByteString().asReadOnlyByteBuffer()))
          + "[" + acl.getAclScope() + "]";
    }

    private Util() {
      // no instances
    }
  }
}
