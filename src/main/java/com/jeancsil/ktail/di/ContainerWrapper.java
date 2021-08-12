package com.jeancsil.ktail.di;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ContainerWrapper {
    private final AnnotationConfigApplicationContext context;
    public ContainerWrapper(final String packages) {
        context = new AnnotationConfigApplicationContext();
        context.scan(packages);
        context.refresh();
    }

    public <T> T getBean(Class<T> name) {
        return context.getBean(name);
    }
}
