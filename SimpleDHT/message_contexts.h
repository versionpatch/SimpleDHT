#pragma once
#include <cstdint>
#include <array>

enum message_context : uint32_t
{
	intro = 0, //machine information, first message of a node
	self_id = 1, //machine information
	record = 2, //array of machine information
	heartbeat = 3, //nothing
	transaction_prepare = 4, //transaction id + transaction data
	transaction_accept = 5, //transaction id
	transaction_refuse = 6, //transaction id
	transaction_commit = 7, //transaction id
	transaction_abort = 8, //transaction id
	sync_ask = 9, //transaction sequence number
	sync_ans = 10, //last sequence number + batch of transactions
	sync_done = 11,
	sync_done_ack = 12,
	network_info_ask = 13,
};

enum class transaction_status : uint8_t
{
	pending,
	aborted,
	committed,
};

struct machine_info
{
	uint32_t host;
	uint16_t port;
	uint64_t id;
	machine_info(uint32_t ip, uint16_t p, uint64_t identifier) : host(ip), port(p), id(identifier)
	{

	}
	machine_info() : host(0), port(0), id(0)
	{

	}
};

namespace message_format
{
	struct intro
	{
		uint64_t id;
		uint16_t port;
	};

	struct sync_ask
	{
		uint64_t id;
		size_t seq;
		size_t lo;
		size_t hi;
	};

	template<size_t replica_count>
	struct join_info_response
	{
		size_t num_entries;
		std::array<machine_info, replica_count> info_array;
	};

}
