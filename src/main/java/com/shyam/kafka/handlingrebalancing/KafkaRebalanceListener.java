package com.shyam.kafka.handlingrebalancing;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class KafkaRebalanceListener implements ConsumerRebalanceListener {

	private KafkaConsumer<String, String> consumer;
	private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();

	public KafkaRebalanceListener(KafkaConsumer<String, String> krc) {
		this.consumer = krc;
	}

	public void addOffset(String topic, int partition, long offset) {
		currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
	}

	public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
		return currentOffsets;
	}

	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		System.out.println("Following Partitions Assigned ....");
		for (TopicPartition partition : partitions)
			System.out.println(partition.partition() + ",");
	}

	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		System.out.println("Following Partitions Revoked ....");
		for (TopicPartition partition : partitions)
			System.out.println(partition.partition() + ",");

		System.out.println("Following Partitions commited ....");
		for (TopicPartition tp : currentOffsets.keySet())
			System.out.println(tp.partition());

		consumer.commitSync(currentOffsets);
		currentOffsets.clear();
	}
}
