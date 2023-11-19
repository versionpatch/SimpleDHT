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
#include <tbb/concurrent_set.h>
#include <tbb/concurrent_vector.h>

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
using sync_caccessor = tbb::concurrent_hash_map<size_t, size_t>::const_accessor;
using sync_accessor = tbb::concurrent_hash_map<size_t, size_t>::accessor;




struct transaction
{
	transaction(size_t uid) : id(uid)
	{

	}

	size_t id;
	size_t key;
	std::vector<char> data;

};

 

class p2p_node
{
public:

	p2p_node(uint16_t port, size_t id);
	~p2p_node();

	
	void start();
	void establish_connection(uint32_t host, uint16_t port);
	void join_ring(uint32_t host, uint16_t port);
	void connect_to_all();
	void read_one_message();
	
	std::optional<machine_info> get_machine_info(size_t id);
	bool cut_connection(size_t machine);
	void cleanup_temp_connections();
	size_t query(size_t key);
	void start_2pc(const transaction& tc);

	//debug
	void log_machine_table();
	void send_sync_to_random_node();

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
	std::optional<connection_ptr> attempt_open_connection(uint32_t host, uint16_t port);
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
	static constexpr std::chrono::duration commit_max_time = std::chrono::milliseconds(10000);
	std::mutex cv_mutex;
	std::condition_variable transaction_cv;
	//This lock is to stop transactions during the final phase of a synchronization. It's basically equivalent a lock on the entire data table.
	std::shared_mutex global_transaction_lock;
	
	//Data handling
	tbb::concurrent_hash_map<size_t, std::vector<char>> data_storage;
	tbb::concurrent_set<size_t> keys;

	//Synchronization
	tbb::concurrent_hash_map<size_t, size_t> sync_progress;
	std::mutex sync_cv_mutex;
	std::condition_variable sync_cv;
	tbb::concurrent_vector<size_t> keys_to_send;
	static constexpr size_t synchronization_batch_size = 128;
	static constexpr std::chrono::duration sync_timeout_time = std::chrono::milliseconds(5000);

	//Join state
	std::mutex join_state_mutex;
	size_t number_of_syncs_finished = 0;
	bool received_network_info = false;
	std::condition_variable join_cv;
	static constexpr std::chrono::duration join_timeout_time = std::chrono::milliseconds(5000);
	static constexpr std::chrono::duration full_sync_timeout_time = std::chrono::milliseconds(600000);

	
	


		

	//personal information
	machine_info my_info;

};