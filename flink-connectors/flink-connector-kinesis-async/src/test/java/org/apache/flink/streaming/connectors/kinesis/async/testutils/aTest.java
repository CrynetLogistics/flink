package org.apache.flink.streaming.connectors.kinesis.async.testutils;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class aTest {

    public static CountDownLatch LATCH = new CountDownLatch(1);

    @ClassRule
    public static GenericContainer KINESIS_CONTAINER = new GenericContainer<>("instructure/kinesalite:latest").withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
        @Override
        public void accept(CreateContainerCmd createContainerCmd) {
            createContainerCmd.withPortBindings(new Ports(new PortBinding(new Ports.Binding("localhost", "14567"), ExposedPort.tcp(4567))));
        }
    });

    @Test
    public void should_send_and_receive_events() throws InterruptedException {
        //System.out.println(KINESIS_CONTAINER.getContainerName());
        //KINESIS_CONTAINER.
        System.out.println(KINESIS_CONTAINER.getDockerImageName());
        System.out.println(KINESIS_CONTAINER.getPortBindings());
        System.out.println(KINESIS_CONTAINER.getEnv());
    }
}
