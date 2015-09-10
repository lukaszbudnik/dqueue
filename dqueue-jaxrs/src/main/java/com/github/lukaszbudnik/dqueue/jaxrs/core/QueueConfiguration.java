package com.github.lukaszbudnik.dqueue.jaxrs.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;

public class QueueConfiguration extends Configuration {

    @JsonProperty(value = "dqueue.env")
    @NotEmpty
    private String env;

    @JsonProperty(value = "dqueue.properties")
    @NotEmpty
    private List<String> properties;

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public List<String> getProperties() {
        return properties;
    }

    public void setProperties(List<String> properties) {
        this.properties = properties;
    }

}
