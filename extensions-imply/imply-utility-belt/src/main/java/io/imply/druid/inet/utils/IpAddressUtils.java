/*
 *
 *  * Copyright (c) Imply Data, Inc. All rights reserved.
 *  *
 *  * This software is the confidential and proprietary information
 *  * of Imply Data, Inc. You shall not disclose such Confidential
 *  * Information and shall use it only in accordance with the terms
 *  * of the license agreement you entered into with Imply.
 *
 *
 */

package io.imply.druid.inet.utils;

import inet.ipaddr.IPAddressSeqRange;
import inet.ipaddr.IPAddressString;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.List;

public class IpAddressUtils
{
  private static char PREFIX_CHARACTER = '/';
  private static char IPV4_CHARACTER = '.';
  private static char IPV6_CHARACTER = ':';
  private static String WILDCARD = "*";


  public static List<IPAddressSeqRange> getPossibleRangesFromIncompleteIp(String incompleteIp) {
    List<IPAddressSeqRange> ranges = new ArrayList<>();
    try {
      if (incompleteIp.indexOf(PREFIX_CHARACTER) != -1) {
        // This is a prefix...
        if (PREFIX_CHARACTER == incompleteIp.charAt(incompleteIp.length() - 1)) {
          // This is a incomplete ip prefix.
          // We will treat this as if the slash doesn't exist yet (which is the same as /32 for ipv4 or /128 for ipv6)
          String ip = StringUtils.chop(incompleteIp);
          ranges.add(new IPAddressString(ip).getAddress().toIPv6().toSequentialRange(new IPAddressString(ip).getAddress().toIPv6()));
        } else {
          // This is a complete ip prefix
          ranges.add(new IPAddressString(incompleteIp).getAddress().toIPv6().toPrefixBlock().toSequentialRange());
        }
      } else {
        // This is an ip
        if (incompleteIp.indexOf(IPV4_CHARACTER) != -1) {
          // This is an ipv4
          int numberOfOctet = StringUtils.countMatches(incompleteIp, IPV4_CHARACTER);
          if (numberOfOctet > 3) {
            throw new IAE("Cannot expand invalid partial address [%s]", incompleteIp);
          }
          if (IPV4_CHARACTER == incompleteIp.charAt(incompleteIp.length() - 1)) {
            // The incomplete ip ends in '.'
            ranges.add(new IPAddressString(incompleteIp + WILDCARD).getAddress().toIPv6().toPrefixBlock().toSequentialRange());
          } else {
            boolean hasFourOctet = numberOfOctet == 3;
            String ipExceptLastPart = incompleteIp.substring(0, incompleteIp.lastIndexOf(IPV4_CHARACTER) + 1);
            String ipLastPart = incompleteIp.substring(incompleteIp.lastIndexOf(IPV4_CHARACTER) + 1);
            ranges.addAll(generatePossibleRangeForIpv4NotEndingInDot(hasFourOctet, ipExceptLastPart, ipLastPart));
          }
        } else if (incompleteIp.indexOf(IPV6_CHARACTER) != -1) {
          // This is an ipv6
          boolean endsInDoubleColon = incompleteIp.endsWith(StringUtils.repeat(IPV6_CHARACTER, 2));
          if (endsInDoubleColon) {
            // This is a complete ipv6
            ranges.add(new IPAddressString(incompleteIp).getAddress().toIPv6().toSequentialRange(new IPAddressString(incompleteIp).getAddress().toIPv6()));
          } else if (IPV6_CHARACTER == incompleteIp.charAt(incompleteIp.length() - 1)) {
            ranges.add(new IPAddressString(incompleteIp + WILDCARD).getAddress().toIPv6().toPrefixBlock().toSequentialRange());
          } else {
            boolean hasEightGroups = incompleteIp.contains(StringUtils.repeat(IPV6_CHARACTER, 2))
                                     || StringUtils.countMatches(incompleteIp, IPV6_CHARACTER) == 7;
            String ipExceptLastPart = incompleteIp.substring(0, incompleteIp.lastIndexOf(IPV6_CHARACTER) + 1);
            String ipLastPart = incompleteIp.substring(incompleteIp.lastIndexOf(IPV6_CHARACTER) + 1);
            ranges.addAll(generatePossibleRangeForIpv6NotEndingInColon(hasEightGroups, ipExceptLastPart, ipLastPart));
          }
        } else {
          // This can be both ipv4 and ipv6
          if (NumberUtils.isDigits(incompleteIp) && Integer.valueOf(incompleteIp) <= 255 && incompleteIp.length() <= 3) {
            // This can be ipv4
            ranges.addAll(generatePossibleRangeForIpv4NotEndingInDot(false, "", incompleteIp));
          }
          // This can be ipv6
          ranges.addAll(generatePossibleRangeForIpv6NotEndingInColon(false, "", incompleteIp));
        }
      }
      return ranges;
    } catch (Exception e) {
      throw new IAE(e, "Cannot expand invalid partial address [%s]", incompleteIp);
    }
  }

  private static List<IPAddressSeqRange> generatePossibleRangeForIpv4NotEndingInDot(boolean hasFourOctet, String ipExceptLastPart, String ipLastPart) {
    List<IPAddressSeqRange> ranges = new ArrayList<>();
    if (!hasFourOctet) {
      ranges.add(new IPAddressString(ipExceptLastPart + ipLastPart + IPV4_CHARACTER + WILDCARD).getAddress().toIPv6().toPrefixBlock().toSequentialRange());
    } else {
      ranges.add(new IPAddressString(ipExceptLastPart + ipLastPart).getAddress().toIPv6().toPrefixBlock().toSequentialRange());
    }
    for (int i = 0; i < 3 - ipLastPart.length(); i++) {
      String ipLastPartLower = ipLastPart + StringUtils.repeat("0", i + 1);
      if (Integer.valueOf(ipLastPartLower) > 255) {
        break;
      }
      String ipLastPartUpper = ipLastPart + StringUtils.repeat("9", i + 1);
      if (Integer.valueOf(ipLastPartUpper) > 255) {
        ipLastPartUpper = "255";
      }
      String rangeString = ipExceptLastPart + ipLastPartLower + "-" + ipLastPartUpper;
      if (!hasFourOctet) {
        rangeString = rangeString + IPV4_CHARACTER + WILDCARD;
      }
      ranges.add(new IPAddressString(rangeString).getAddress().toIPv6().toPrefixBlock().toSequentialRange());
    }
    return ranges;
  }

  private static List<IPAddressSeqRange> generatePossibleRangeForIpv6NotEndingInColon(boolean hasEightGroups, String ipExceptLastPart, String ipLastPart) {
    List<IPAddressSeqRange> ranges = new ArrayList<>();
    if (!hasEightGroups) {
      ranges.add(new IPAddressString(ipExceptLastPart + ipLastPart + IPV6_CHARACTER + WILDCARD).getAddress().toIPv6().toPrefixBlock().toSequentialRange());
    } else {
      ranges.add(new IPAddressString(ipExceptLastPart + ipLastPart).getAddress().toIPv6().toPrefixBlock().toSequentialRange());
    }
    for (int i = 0; i < 4 - ipLastPart.length(); i++) {
      String ipLastPartLower = ipLastPart + StringUtils.repeat("0", i + 1);
      String ipLastPartUpper = ipLastPart + StringUtils.repeat("F", i + 1);
      String rangeString = ipExceptLastPart + ipLastPartLower + "-" + ipLastPartUpper;
      if (!hasEightGroups) {
        rangeString = rangeString + IPV6_CHARACTER + WILDCARD;
      }
      ranges.add(new IPAddressString(rangeString).getAddress().toIPv6().toPrefixBlock().toSequentialRange());
    }
    return ranges;
  }
}
