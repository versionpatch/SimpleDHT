#pragma once

#include <asio.hpp>
#include <string>
#include "Utils.h"


struct MessageHeader
{
	uint32_t context;
	uint32_t size = 0;
};
struct Message
{
	MessageHeader header;
	std::vector<char> data;
};

template<typename T>
Message build_message(uint32_t context, const T &data)
{
	static_assert(std::is_standard_layout<T>::value == true);
	Message m;
	m.header.context = context;
	m.header.size = sizeof(T);
	m.data.resize(sizeof(T));
	std::memcpy(m.data.data(), &data, sizeof(T));
	return m;
}

template<typename T>
void read_message_data(T& val, const Message &m)
{
	std::memcpy(&val, m.data.data(), sizeof(T));
}

template<typename T>
Message build_message_array(uint32_t context, const std::vector<T>& content)
{
	static_assert(std::is_standard_layout<T>::value == true);
	Message m;
	m.header.context = context;
	m.header.size = sizeof(T) * content.size();
	m.data.resize(sizeof(T) * content.size());
	std::memcpy(m.data.data(), content.data(), sizeof(T) * content.size());
	return m;
}

template<typename T>
void read_message_array(std::vector<T>& dest, const Message& m)
{
	dest.resize(m.header.size / sizeof(T));
	std::memcpy(dest.data(), m.data.data(), m.header.size);
}

class TCPConnection;

using TaggedMessage = std::pair<std::shared_ptr<TCPConnection>, Message>;

class TCPConnection : public std::enable_shared_from_this<TCPConnection>
{

public:

	TCPConnection(asio::io_context& asio_context, asio::ip::tcp::socket sockt, utils::sync_queue<TaggedMessage>& msg_queue) :
		context(asio_context),
		socket(std::move(sockt)),
		in_messages(msg_queue),
		hostname(socket.remote_endpoint().address().to_v4().to_ulong())
	{
		temp_message = Message();
	}
	~TCPConnection()
	{
		close();
	}

	bool active()
	{
		return socket.is_open();
	}

	void start_connection()
	{
		read_header();
	}

	void close()
	{
		if (socket.is_open())
		{
			asio::post(context, [this]()
				{
					socket.close();
			});
		}
	}

	uint32_t get_hostname()
	{
		return hostname;
	}

	void send_message(const Message &msg)
	{
		asio::post(context,
			[this, msg]()
			{
				bool was_empty = out_messages.empty();
				out_messages.push(msg);
				if (was_empty)
					write_header();
			}
		);
	}

private:

	void push_message()
	{
		in_messages.push(std::make_pair(this->shared_from_this(), temp_message));
	}

	void write_header()
	{
		auto handle = this->shared_from_this();
		asio::async_write(socket, asio::buffer(&out_messages.front()->header, sizeof(MessageHeader)),
			[this, handle](std::error_code ec, size_t length)
			{
				if (!ec)
				{
					if (out_messages.front()->header.size > 0)
					{
						write_data();
					}
					else
					{
						out_messages.pop();
						if (!out_messages.empty())
							write_header();
					}

				}
				else
				{
					std::cout << "[ERROR] " << hostname << " : Failure while trying to write header : " << ec.message() << "\n";
					if (socket.is_open())
						socket.close();
				}
			}
		);
	}
	void write_data()
	{
		auto handle = this->shared_from_this();
		asio::async_write(socket, asio::buffer(out_messages.front()->data.data(), out_messages.front()->header.size),
			[this, handle](std::error_code ec, size_t length)
			{
				if (!ec)
				{
					out_messages.pop();
					if (!out_messages.empty())
						write_header();
				}
				else
				{
					std::cout << hostname << " : Failure while trying to write data : " << ec.message() << "\n";
					if (socket.is_open())
						socket.close();
				}
			}
		);
	}

	void read_header()
	{
		auto handle = this->shared_from_this();
		asio::async_read(socket, asio::buffer(&temp_message.header, sizeof(MessageHeader)), 
			[this, handle](std::error_code ec, size_t length)
			{
				if (!ec)
				{
					if (temp_message.header.size > 0)
					{
						temp_message.data.resize(temp_message.header.size);
						read_data();
					}
					else
					{
						push_message();
						read_header();
					}
				}
				else
				{
					std::cout << hostname << " : Failure while trying to read header : " << ec.message() << "\n";
					if (socket.is_open())
						socket.close();
				}
			}
		);
	}
	void read_data()
	{
		auto handle = this->shared_from_this();
		asio::async_read(socket, asio::buffer(temp_message.data.data(), temp_message.header.size),
			[this, handle](std::error_code ec, size_t length)
			{
				if (!ec)
				{
					push_message();
					read_header();
				}
				else
				{
					std::cout << hostname << " : Failure while trying to read data : " << ec.message() << "\n";
					if (socket.is_open())
						socket.close();
				}
			}
		);
	}

private:
	//ASIO
	asio::io_context& context;
	asio::ip::tcp::socket socket;

	//Message queues
	utils::sync_queue<TaggedMessage>& in_messages;
	utils::sync_queue<Message> out_messages;

	//Message reception
	Message temp_message;

	//Identifiers
	uint32_t hostname;

};