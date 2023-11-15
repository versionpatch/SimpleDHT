#pragma once
#include <cstdint>

enum message_context : uint32_t
{
	intro = 0,
	self_id = 1,
	record = 2,
	heartbeat = 3,
	transaction_prepare = 4,
	transaction_accept = 5,
	transaction_refuse = 6,
	transaction_commit = 7,
	transaction_abort = 8,
};

enum class transaction_status : uint8_t
{
	pending,
	aborted,
	committed,
};

namespace message_format
{
	struct intro
	{
		uint64_t id;
		uint16_t port;
	};
}
