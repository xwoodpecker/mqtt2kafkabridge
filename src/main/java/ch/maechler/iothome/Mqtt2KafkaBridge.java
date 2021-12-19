package ch.maechler.iothome;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.apache.logging.log4j.*;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.SocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.swing.text.html.Option;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Optional;
import java.util.Properties;

public class Mqtt2KafkaBridge implements MqttCallbackExtended {
    private final Logger logger = LogManager.getLogger(Mqtt2KafkaBridge.class);
    private KafkaProducer<String, byte[]> kafkaProducer;
    private MqttClient mqttClient;
    private String mqttTopicSeparator, kafkaTopicSeparator;
    private String[] mqttTopics;
    private boolean kafkaIncludeTopicName;

    public static void main(String[] args) {
        new Mqtt2KafkaBridge().run();
    }

    private void run() {
        logger.info("Start to run Mqtt2KafkaBridge.");

        Properties kafkaProducerProperties = new Properties();
        String clientId = Optional.ofNullable(System.getenv("CLIENT_ID")).orElse("Mqtt2KafkaBridge");
        String kafkaHost = Optional.ofNullable(System.getenv("KAFKA_BROKER_HOST")).orElse("localhost:9092");
        String mqttBrokerHost = Optional.ofNullable(System.getenv("MQTT_BROKER_HOST")).orElse("localhost:1883");
        String mqttBrokerUser = Optional.ofNullable(System.getenv("MQTT_BROKER_USER")).orElse("");
        String mqttBrokerPassword = Optional.ofNullable(System.getenv("MQTT_BROKER_PASSWORD")).orElse("");
        Boolean mqttAutomaticReconnect = Boolean.parseBoolean(Optional.ofNullable(System.getenv("MQTT_AUTOMATIC_RECONNECT")).orElse("true"));
        String mqttTopicsString = Optional.ofNullable(System.getenv("MQTT_TOPIC_FILTER")).orElse("#");
        mqttTopics = mqttTopicsString.split(",");
        mqttTopicSeparator = Optional.ofNullable(System.getenv("MQTT_TOPIC_SEPARATOR")).orElse("/");
        kafkaTopicSeparator = Optional.ofNullable(System.getenv("KAFKA_TOPIC_SEPARATOR")).orElse(".");
        // added with fork:
        Boolean mqttCleanSession = Boolean.parseBoolean(Optional.ofNullable(System.getenv("MQTT_CLEAN_SESSION")).orElse("true"));
        Integer mqttMaxInflight = Integer.parseInt(Optional.ofNullable(System.getenv("MQTT_MAX_INFLIGHT")).orElse("10"));
        String mqttClientCaFile = Optional.ofNullable(System.getenv("MQTT_CLIENT_CA_FILE")).orElse("");
        String mqttClientCertFile = Optional.ofNullable(System.getenv("MQTT_CLIENT_CERT_FILE")).orElse("");
        String mqttClientKeyFile = Optional.ofNullable(System.getenv("MQTT_CLIENT_KEY_FILE")).orElse("");
        kafkaIncludeTopicName = Boolean.parseBoolean(Optional.ofNullable(System.getenv("KAFKA_INCLUDE_TOPIC_NAME")).orElse("false"));

        logger.info(
            "Configuration values: \n " +
                    "CLIENT_ID={} \n " +
                    "KAFKA_HOST={} \n " +
                    "KAFKA_TOPIC_SEPARATOR={} \n " +
                    "MQTT_BROKER_HOST={} \n " +
                    "MQTT_BROKER_USER={} \n " +
                    "MQTT_AUTOMATIC_RECONNECT={} \n " +
                    "MQTT_TOPIC_SEPARATOR={} \n " +
                    "MQTT_TOPIC_FILTER={} \n" +
                    "MQTT_CLEAN_SESSION={} \n" +
                    "MQTT_MAX_INFLIGHT={} \n" +
                    "KAFKA_INCLUDE_TOPIC_NAME={} \n",
                clientId, kafkaHost, kafkaTopicSeparator, mqttBrokerHost, mqttBrokerUser, mqttAutomaticReconnect,
                mqttTopicSeparator, mqttTopics, mqttCleanSession, mqttMaxInflight, kafkaIncludeTopicName
        );

        kafkaProducerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        kafkaProducerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        try {
            logger.info("Connecting to MQTT and Kafka broker.");

            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);

            mqttConnectOptions.setUserName(mqttBrokerUser);
            mqttConnectOptions.setPassword(mqttBrokerPassword.toCharArray());
            mqttConnectOptions.setAutomaticReconnect(mqttAutomaticReconnect);
            //added CleanSession and SocketFactory
            mqttConnectOptions.setCleanSession(mqttCleanSession);
            mqttConnectOptions.setMaxInflight(mqttMaxInflight);

            boolean useSSL = false;
            if(!mqttClientCaFile.isEmpty() && !mqttClientCertFile.isEmpty() && !mqttClientKeyFile.isEmpty()) {
                SocketFactory sf = getSSLSocketFactory(mqttClientCaFile, mqttClientCertFile, mqttClientKeyFile);
                if(sf != null) {
                    mqttConnectOptions.setSocketFactory(sf);
                    useSSL = true;
                }
            }

            String protocol = useSSL ? "ssl" : "tcp";
            // generate a new clientId in case a clean session is used
            if(mqttCleanSession)
                clientId = MqttClient.generateClientId();
            mqttClient = new MqttClient(protocol + "://"+ mqttBrokerHost, clientId, new MemoryPersistence());
            mqttClient.setCallback(this);
            mqttClient.connect(mqttConnectOptions);

            logger.info("Connected to broker, ready to bridge!");
        } catch(MqttException e) {
            logger.error("Oops, an error occurred.", e);
        }
    }


    /**
     * Gets socket factory for certificate files
     *
     * @param caFile         the ca file
     * @param clientCertFile the client cert file
     * @param clientKeyFile  the client key file
     * @return the socket factory
     * @throws Exception the exception
     */
    private  SSLSocketFactory getSSLSocketFactory(final String caFile, final String clientCertFile, final String clientKeyFile) {

        SSLSocketFactory socketFactory = null;
        try {
            socketFactory = getSSLContext(caFile, clientCertFile, clientKeyFile).getSocketFactory();
        } catch (Exception e) {
            logger.error("Oops, could not create socketfactory.", e);
        }
        return socketFactory;
    }

    /**
     * Gets ssl context for given certificates
     *
     * @param caFile         the ca file
     * @param clientCertFile the ca file
     * @param clientKeyFile  the client key file
     * @return ssl context
     * @throws Exception the exception
     */
    public SSLContext getSSLContext(final String caFile, final String clientCertFile, final String clientKeyFile) throws Exception {


        Security.addProvider(new BouncyCastleProvider());

        // load CA certificate
        X509Certificate caCert = null;
        CertificateFactory cf;
        try(InputStream stream = new ByteArrayInputStream(Base64.getDecoder().decode(caFile))) {
            try (BufferedInputStream bis = new BufferedInputStream(stream)) {

                cf = CertificateFactory.getInstance("X.509");

                while (bis.available() > 0) {
                    caCert = (X509Certificate) cf.generateCertificate(bis);
                }
            }
        }

        X509Certificate cert = null;
        // load client certificate
        try (InputStream stream = new ByteArrayInputStream(Base64.getDecoder().decode(clientCertFile))) {
            try (BufferedInputStream bis = new BufferedInputStream(stream)) {
                while (bis.available() > 0) {
                    cert = (X509Certificate) cf.generateCertificate(bis);
                }
            }
        }

        // load client private key
        byte[] keyArray = Base64.getDecoder().decode(clientKeyFile);
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyArray);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        PrivateKey privateKey =  kf.generatePrivate(spec);

        // CA certificate is used to authenticate server
        KeyStore caKs = KeyStore.getInstance(KeyStore.getDefaultType());
        caKs.load(null, null);
        caKs.setCertificateEntry("ca-certificate", caCert);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(caKs);

        // client key and certificates are sent to server so it can authenticate us
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        ks.setCertificateEntry("certificate", cert);
        ks.setKeyEntry("private-key", privateKey, null, new java.security.cert.Certificate[] { cert });
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX");
        kmf.init(ks, null);

        // finally, create SSL socket factory
        SSLContext context = SSLContext.getInstance("TLSv1.2");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        return context;
    }

    @Override
    public void connectionLost(Throwable cause) {
        logger.error("Connection to MQTT server lost.", cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        logger.debug("MQTT message arrived: {} -> {} [{}]", topic, message.getId(), message);

        String kafkaTopic = topic.replace(mqttTopicSeparator, kafkaTopicSeparator);

        ProducerRecord producerRecord = new ProducerRecord<String, byte[]>(kafkaTopic, message.getPayload());
        if(kafkaIncludeTopicName) {
            String json;
            ObjectMapper mapper = new ObjectMapper();
            try {
                KafkaMessage kafkaMessage = new KafkaMessage(topic, new String(message.getPayload()));
                json = mapper.writeValueAsString(kafkaMessage);
                producerRecord = new ProducerRecord<String, String>(kafkaTopic, json);
            } catch (JsonProcessingException e) {
                logger.error("Oops, an error occurred while creating json for message. \n {}", message.getPayload(), e);
            }
        }
        Callback callback = (RecordMetadata metadata, Exception e) -> {
            if (e == null) {
                logger.trace("Message sent to Kafka: {} -> {}", kafkaTopic, message.getId());
            } else {
                logger.error("Oops, an error occurred while sending message to Kafka. \n {} -> {}", kafkaTopic, message.getId(), e);
            }
        };

        kafkaProducer.send(producerRecord, callback);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        logger.warn("Delivery complete. This method should actually never be called. Token: {}", token);
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        logger.info("Connect to MQTT broker '{}' complete. Reconnect={}", serverURI, reconnect);

        try {
            logger.info("Subscribe to topics {}.", mqttTopics);
            mqttClient.subscribe(mqttTopics);
        } catch(MqttException e) {
            logger.error("Oops, an error occurred.", e);
        }
    }
}