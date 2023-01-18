package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;

@JsonDeserialize(builder = ConnectionParseSchemaProvider.Builder.class)
public class ConnectionParseSchemaProvider extends ParseSchemaProvider {

  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid String connectionName;

  public ConnectionParseSchemaProvider(
      final @NotNull ParseSchemaProviderType type,
      final String connectionName
  ) {
    super(type);
    this.connectionName = connectionName;
  }

  public static ConnectionParseSchemaProvider.Builder builder() {
    return new ConnectionParseSchemaProvider.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public ConnectionParseSchemaProvider.Builder with() {
    return (new Builder())
        .type(this.getType())
        .connectionName(this.getConnectionName());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public ConnectionParseSchemaProvider with(final java.util.function.Consumer<Builder> consumer) {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public ConnectionParseSchemaProvider cloneWithReadOnlyDefaults() {
    return (new Builder())
        .connectionName(this.getConnectionName())
        .type(this.getType())
        .build();
  }




  @JsonProperty("connectionName")
  public String getConnectionName() {
    return connectionName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConnectionParseSchemaProvider connectionParseSchemaProvider = (ConnectionParseSchemaProvider) o;
    return Objects.equals(this.connectionName, connectionParseSchemaProvider.connectionName) &&
           super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionName, super.hashCode());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ConnectionParseSchemaProvider {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    connectionName: ").append(toIndentedString(connectionName)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(final Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

  @Override
  public void accept(final ParseSchemaProvider.Visitor visitor) {
    visitor.visit(this);
  }


  @JsonPOJOBuilder
  public static class Builder implements ParseSchemaProvider.IBuilder<ConnectionParseSchemaProvider> {
    private @Valid ParseSchemaProviderType type;
    private @Valid String connectionName;

    /**
     * Set type and return the builder.
     */
    @Override
    @JsonProperty("type")
    public Builder type(final @Valid  @NotNull
                        ParseSchemaProviderType type) {
      this.type = type;
      return this;
    }


    /**
     * Set connectionName and return the builder.
     * Name of the connection that provides the parse schema.
     */
    @JsonProperty("connectionName")
    public Builder connectionName(final @Valid String connectionName) {
      this.connectionName = connectionName;
      return this;
    }



    @Override
    public ConnectionParseSchemaProvider build() {
      return new ConnectionParseSchemaProvider(type, connectionName);
    }
  }
}
