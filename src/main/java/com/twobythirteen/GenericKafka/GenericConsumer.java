package com.twobythirteen.GenericKafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;


public class GenericConsumer {
	static final Logger LOGGER = LoggerFactory.getLogger(GenericConsumer.class);

	private static String bootstrapServers = "localhost:9092";
	private static String topic = "localhost_bootstrap_test_20170525";
	// this + auto.offset.reset = "earliest" means we start at the beginning every time
	// this pollutes zookeeper though... ok for dev. not good for integration or prod.
	private static String consumerGroup = UUID.randomUUID().toString();

	private static String tableSchema = "SCHEMA";
	// TODO/NOTE: serverTimezone=UTC
	private static String connectionString = "jdbc:mysql://localhost:3306/" + tableSchema + "?serverTimezone=UTC";
	private static String user = "USER";
	private static String password = "PASSWORD";


	private org.apache.kafka.clients.consumer.KafkaConsumer consumer;
	private List<String> topics;
	private long timeout = 5000;

	public GenericConsumer(String bootstrapServers, String topic, String groupId) {
		LOGGER.debug("Bootstrap servers: " + bootstrapServers);
		LOGGER.debug("Topic: " + topic);

		this.topics = Arrays.asList(topic);
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("group.id", groupId);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("auto.offset.reset", "earliest");

		LOGGER.info("Creating Consumer with properties: " + props.toString());
		this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);
	}

	public HashMap<String, JSONArray> consumeTopics() {
		HashMap<String, JSONArray> kafkaData = new HashMap<String, JSONArray>();
		try {
			consumer.subscribe(this.topics);
//			consumer.subscribe(topics, new ConsumerRebalanceListener() {
//				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//					// NoOp
//				}
//
//				/**
//				 * Always go to the beginning of the partitions to ensure we account for all
//				 * consumers.
//				 */
//				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//					consumer.seekToBeginning(partitions);
//				}
//			});

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(timeout);
				if (records.count() == 0) {
					break;
				}
				for (ConsumerRecord<String, String> record : records) {
					JSONObject rec = new JSONObject(record.value());
					String type = rec.getString("type");
					if (type.equalsIgnoreCase("bootstrap-insert")) {
						String table = rec.getString("table");
						kafkaData.put(table, kafkaData.getOrDefault(table, new JSONArray()).put(rec));
					}

				}
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			consumer.close();
		}

		return kafkaData;
	}

	/**
	 * Ensure we exit the consumer correctly.
	 */
	public void shutdown() {
		consumer.wakeup();
	}


	public static void main(String[] args) {
		try {
			// connect to the db first just to make sure it's accessible before we get too far down the road
			LOGGER.info("connecting to sql");
			SqlConnector sqlConnector = new SqlConnector(connectionString, user, password);

			LOGGER.info("connecting to kafka");
			GenericConsumer genericConsumer = new GenericConsumer(bootstrapServers, topic, consumerGroup);

			LOGGER.info("consuming topics");
			HashMap<String, JSONArray> kafkaData = genericConsumer.consumeTopics();

			LOGGER.info("iterating over kafka data");
			Iterator<Map.Entry<String, JSONArray>> it = kafkaData.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, JSONArray> kafkaTable = it.next();
				String tableName = kafkaTable.getKey();
				LOGGER.info("processing " + tableName);
				LOGGER.info("extracting table");
				HashMap<String, JSONArray> tableData = sqlConnector.extractTable(tableSchema, tableName);
				LOGGER.info("comparing table to kafka");
				sqlConnector.compareTableToKafka(tableName, tableData, kafkaTable.getValue());
				LOGGER.info("moving on");
				tableData.clear();
				it.remove();
			}
			LOGGER.info("done iterating over kafka data");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
