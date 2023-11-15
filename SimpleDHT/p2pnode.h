#pragma once

#include <asio.hpp>
#include <string>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <mutex>
#include <shared_mutex>
#include <iostream>
#include <thread>
#include <random>
#include <tbb/concurrent_hash_map.h>

#include "NodeTable.h"
#include "Utils.h"
#include "TCPConnection.h"
#include "message_contexts.h"



static constexpr uint32_t localhost = 2130706433;
using connection_ptr = std::shared_ptr<TCPConnection>;
using tcp = asio::ip::tcp;

using xans_caccessor = tbb::concurrent_hash_map<size_t, std::unordered_set<size_t>>::const_accessor;
using xans_accessor = tbb::concurrent_hash_map<size_t, std::unordered_set<size_t>>::accessor;
using xstatus_caccessor = tbb::concurrent_hash_map<size_t, transaction_status>::const_accessor;
using xstatus_accessor = tbb::concurrent_hash_map<size_t, transaction_status>::accessor;
using data_caccessor = tbb::concurrent_hash_map<size_t, std::vector<char>>::const_accessor;
using data_accessor = tbb::concurrent_hash_map<size_t, std::vector<char>>::accessor;


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

struct write_entry
{
	size_t key;
	std::vector<char> data;
	write_entry(size_t k) : key(k)
	{

	}
};
struct transaction
{
	transaction(size_t uid) : id(uid)
	{

	}

	size_t id;
	std::vector<write_entry> records;
};


class p2p_node
{
public:

	p2p_node(uint16_t port, size_t id);
	~p2p_node();

	
	void start();
	void establish_connection(uint32_t host, uint16_t port);
	void connect_to_all();
	void read_one_message();
	
	std::optional<machine_info> get_machine_info(size_t id);
	bool cut_connection(size_t machine);
	void cleanup_temp_connections();
	size_t query(size_t key);
	void start_2pc(const transaction& tc);

	//debug
	void log_machine_table();

private:

	//Connection handling data structures
	std::map<size_t, machine_info> machines;
	std::unordered_map<size_t, connection_ptr> established_connections;
	std::unordered_map<connection_ptr, size_t> connection_to_id;
	std::vector<size_t> connected_ids;
	void add_new_machine(connection_ptr ptr, uint64_t id, uint16_t port);
	void add_new_machine(const machine_info& inf);
	void broadcast(const Message &m);
	void cleanup_connection_tables();
	int get_node_index(size_t id);
	std::shared_mutex table_lock;

	std::vector<connection_ptr> temp_connections;
	std::shared_mutex connections_lock;

	//ASIO
	asio::io_context context;
	std::thread context_thrd;
	tcp::acceptor acceptor;
	utils::sync_queue<TaggedMessage> in_messages;
	void accept_connection();

	//Garbage collection
	asio::steady_timer garbage_collection_timer;
	static constexpr std::chrono::duration garbage_collection_interval = std::chrono::milliseconds(5000);
	void handle_garbage_collection();

	//Heartbeat handling
	asio::steady_timer heartbeat_send_timer;
	asio::steady_timer heartbeat_process_timer;
	static constexpr std::chrono::duration heartbeat_send_interval = std::chrono::milliseconds(500);
	static constexpr std::chrono::duration heartbeat_check_interval = std::chrono::milliseconds(10000);
	std::shared_mutex heartbeat_lock;
	std::unordered_set<size_t> heartbeat_ids;
	void register_heartbeat(size_t id);
	void handle_heartbeat();
	void broadcast_heartbeat();
	
	//Transaction handling
	static constexpr size_t replication_count = 2;
	tbb::concurrent_hash_map<size_t, std::unordered_set<size_t>> transaction_ans;
	tbb::concurrent_hash_map<size_t, transaction_status> transaction_status_table;
	static constexpr std::chrono::duration commit_max_time = std::chrono::milliseconds(5000);
	std::mutex cv_mutex;
	std::condition_variable transaction_cv;
	
	//Data handling
	tbb::concurrent_hash_map<size_t, std::vector<char>> data_storage;
	


		

	//personal information
	machine_info my_info;

};