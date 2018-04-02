package com.ainory.kafka.streams.process;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * @author ainory on 2018. 3. 23..
 */
public class ProcessorSupplierTest implements ProcessorSupplier {
    @Override
    public Processor get() {
        return new ProcessTest1();
    }
}
