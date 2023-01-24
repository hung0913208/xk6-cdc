/**
 * @class       : cdc
 * @author      : Hung Nguyen Xuan Pham (hung0913208@gmail.com)
 * @created     : Tuesday Jan 24, 2023 12:50:53 +07
 * @description : cdc
 */

import { Writer, Reader } from 'k6/x/cdc';

const brokers = ["kafka:9092"];
const topic = "xk6_kafka_consumer_group_topic";

const writer = new Writer({
	kafka: {
		servers: ["kafka:9092"],
		timeout: 1000,
		debug: true
	},
	pg: {
		host: "postgres",
		port: 5432,
		dbname: "test-event",
		user: "eventuate",
		password: "eventuate"
	}
})

const reader = new Reader({
	servers: ["kafka:9092"],
	topics: ["test-event"],
	timeout: 1000,
	debug: true
})

writer.publish({
	topic: "test-event",
	value: "test"
})

writer.insert({
	table: "test-event",
	feilds: [
		[1, "abc", 123],
		[2, "def", 456]
	]
})
