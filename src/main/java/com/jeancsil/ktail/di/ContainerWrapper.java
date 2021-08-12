package com.jeancsil.ktail.di;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Just a container wrapper to make the Main code
 * cleaner.
 * It encapsulates the context object (AnnotationConfigApplicationContext).
 */
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
