package io.imply.druid.sql.calcite.external;

import org.apache.calcite.sql.SqlCall;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.external.DruidTableMacro;
import org.apache.druid.sql.calcite.external.DruidUserDefinedTableMacro;
import org.apache.druid.sql.calcite.external.Externals;

import java.util.Collections;
import java.util.Set;

public class PolarisSourceTableMacro extends DruidUserDefinedTableMacro
{
  public PolarisSourceTableMacro(DruidTableMacro macro)
  {
    super(macro);
  }

  @Override
  public Set<ResourceAction> computeResources(final SqlCall call, boolean inputSourceTypeSecurityEnabled)
  {
    if (!inputSourceTypeSecurityEnabled) {
      return Collections.singleton(Externals.EXTERNAL_RESOURCE_ACTION);
    }
    return Collections.singleton(new ResourceAction(new Resource(
        ResourceType.EXTERNAL,
        BasePolarisInputSourceDefn.TYPE_KEY
    ), Action.READ));
  }
}
