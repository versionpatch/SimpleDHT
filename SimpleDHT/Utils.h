#pragma once
#include <string>
#include <sstream>
#include <queue>
#include <mutex>
#include <optional>

namespace utils
{
	template<typename T>
	class sync_queue
	{
	public:
		bool empty()
		{
			std::lock_guard<std::mutex> lock(queue_mutex);
			return queue.empty();
		}
		void push(T data)
		{
			std::lock_guard<std::mutex> lock(queue_mutex);
			queue.push(data);
		}
		std::optional<T> front()
		{
			std::lock_guard<std::mutex> lock(queue_mutex);
			if (queue.empty())
				return std::nullopt;
			return queue.front();
		}
		std::optional<T> pop()
		{
			std::lock_guard<std::mutex> lock(queue_mutex);
			if (queue.empty())
				return std::nullopt;
			auto val = queue.front();
			queue.pop();
			return val;
		}
	private:
		
		std::queue<T> queue;
		std::mutex queue_mutex;
	};

}