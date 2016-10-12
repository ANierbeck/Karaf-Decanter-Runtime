package de.nierbeck.decanter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.osgi.framework.BundleContext;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

@Component(immediate=true, service = {})
public class DecanterEnabler {

    final static private Logger LOG = LoggerFactory.getLogger(DecanterEnabler.class);
    
    final private Socket sock = new Socket();
    final private int timeOut = (int) TimeUnit.SECONDS.toMillis(1);

    private ConfigurationAdmin configAdmin;
    
    @Activate
    public void start(BundleContext bundleContext) {
        LOG.info("starting DecanterEnabler, will scan for Cassandra and local Kafka ...");
        
        if (isKafkaRunningLocaly()) {
            LOG.info("Found local Kafka running");
            configureKafkaProcessCollector();
        } 
        
        if (isCassandraRunningLocaly()) {
            LOG.info("Found local Cassandra running");
            configureCassandraCollector();
        }
    }

    private void configureCassandraCollector() {
        try {
            Configuration configuration = configAdmin.createFactoryConfiguration("org.apache.karaf.decanter.collector.jmx");

            Dictionary<String, Object> dictionary = configuration.getProperties();
            if (dictionary == null) {
                dictionary = new Hashtable<String, Object>();
            }

            dictionary.put("type", "jmx-cassandra");
            dictionary.put("url", "service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi");
            dictionary.put("object.name", "org.apache.cassandra.metrics:type=ClientRequest,scope=*,name=*");
            dictionary.put("server_ip", InetAddress.getLocalHost().toString());

            LOG.info("updating jmx-cassandra configuration");

            configuration.setBundleLocation(null);
            configuration.update(dictionary);

        } catch (IOException e) {
            LOG.error("Can't configure configuration for jmx-cassandra");
        }
    }
    
    private void configureKafkaProcessCollector() {
        try {
            Configuration configuration = configAdmin.createFactoryConfiguration("org.apache.karaf.decanter.collector.process");

            Dictionary<String, Object> dictionary = configuration.getProperties();
            if (dictionary == null) {
                dictionary = new Hashtable<String, Object>();
            }

            dictionary.put("type", "process-jmx");
            dictionary.put("process", "Kafka");
            dictionary.put("object.name", "kafka.server:type=BrokerTopicMetrics,name=*");
            dictionary.put("server_ip", InetAddress.getLocalHost().toString());

            LOG.info("updating jmx-cassandra configuration");

            configuration.setBundleLocation(null);
            configuration.update(dictionary);

        } catch (IOException e) {
            LOG.error("Can't configure configuration for jmx-cassandra");
        }
    }

    @Deactivate
    public void stop(BundleContext bundleContext) {

    }
    
    @Reference(unbind = "unsetConfigAdminService")
    protected void setConfigAdminService(ConfigurationAdmin ca) {
        this.configAdmin = ca;
    }

    protected void unsetConfigAdminService(ConfigurationAdmin ca) {
        this.configAdmin = null;
    }

    private boolean isCassandraRunningLocaly() {
        try {
            sock.connect(new InetSocketAddress("localhost", 7199), timeOut);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
    
    
    private boolean isKafkaRunningLocaly() {
        List<VirtualMachineDescriptor> collect = VirtualMachine.list().stream().filter(descriptor -> descriptor.displayName().contains("Kafka")).collect(Collectors.toList());
        return !collect.isEmpty();
    }
    
}
