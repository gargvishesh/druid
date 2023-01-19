/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class transforms the values of TIMESTAMP or DATE type for sql query results.
 * The transformation is required only when the sql query is submitted to {@link org.apache.druid.sql.http.SqlResource}.
 */
public class SqlRowTransformer
{
  private final DateTimeZone timeZone;
  private final RelDataType rowType;
  private final List<String> fieldList;

  // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
  private final boolean[] timeColumns;
  private final boolean[] dateColumns;

  private final boolean[] arrayColumns;

  SqlRowTransformer(DateTimeZone timeZone, RelDataType rowType)
  {
    this.timeZone = timeZone;
    this.rowType = rowType;
    this.fieldList = new ArrayList<>(rowType.getFieldCount());
    this.timeColumns = new boolean[rowType.getFieldCount()];
    this.dateColumns = new boolean[rowType.getFieldCount()];
    this.arrayColumns = new boolean[rowType.getFieldCount()];
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      final SqlTypeName sqlTypeName = rowType.getFieldList().get(i).getType().getSqlTypeName();
      if (sqlTypeName == SqlTypeName.ARRAY) {
        ArraySqlType arraySqlType = (ArraySqlType) rowType.getFieldList().get(i).getType();
        SqlTypeName elementSqlTypeName = arraySqlType.getComponentType().getSqlTypeName();
        timeColumns[i] = elementSqlTypeName == SqlTypeName.TIMESTAMP;
        dateColumns[i] = elementSqlTypeName == SqlTypeName.DATE;
        arrayColumns[i] = true;
      } else {
        timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
        dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
        arrayColumns[i] = false;
      }
      fieldList.add(rowType.getFieldList().get(i).getName());
    }
  }

  public RelDataType getRowType()
  {
    return rowType;
  }

  public List<String> getFieldList()
  {
    return fieldList;
  }

  @Nullable
  public Object transform(Object[] row, int i)
  {
    if (row[i] == null) {
      return null;
    } else if (arrayColumns[i]) {
      String str = ((String) row[i]);
      str = str.substring(1, str.length()-1);
      String[] strArray = str.split(",");

      String[] resultArray = new String[strArray.length];

      if (timeColumns[i]) {
        for (int j = 0; j < resultArray.length; j++) {
          resultArray[j] = ISODateTimeFormat.dateTime().print(
              Calcites.calciteTimestampToJoda(Long.parseLong(strArray[j]), timeZone)
          );
        }
        return resultArray;
      } else if (dateColumns[i]) {
        for (int j = 0; j < resultArray.length; j++) {
          resultArray[j] = ISODateTimeFormat.dateTime().print(
              Calcites.calciteDateToJoda(Integer.parseInt(strArray[i]), timeZone)
          );
        }
        return resultArray;
      } else {
        return row[i];
      }
    } else if (timeColumns[i]) {
      return ISODateTimeFormat.dateTime().print(
          Calcites.calciteTimestampToJoda((long) row[i], timeZone)
      );
    } else if (dateColumns[i]) {
      return ISODateTimeFormat.dateTime().print(
          Calcites.calciteDateToJoda((int) row[i], timeZone)
      );
    } else {
      return row[i];
    }
  }
}
