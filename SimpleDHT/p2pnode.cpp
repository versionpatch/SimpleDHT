#include "p2pnode.h"
#include <chrono>


//helpers
char* encode_transaction(char* dest, const transaction &tc)
{
	std::memcpy(dest, &tc.id, sizeof(uint64_t));
	dest += sizeof(uint64_t);
	std::memcpy(dest, &tc.key, sizeof(uint64_t));
	dest += sizeof(uint64_t);
	size_t record_size = tc.data.size();
	std::memcpy(dest, &record_size, sizeof(uint64_t));
	dest += sizeof(uint64_t);
	std::memcpy(dest, tc.data.data(), record_size);
	dest += record_size;
	return dest;
}

Message encode_transaction(const transaction& tc)
{
	Message m;
	m.header.context = message_context::transaction_prepare;
	m.header.size = 3 * sizeof(uint64_t) + sizeof(uint64_t) + tc.data.size();
	m.data.resize(m.header.size);
	encode_transaction(m.data.data(), tc);
	return m;
}

const char* decode_transaction(transaction &dest, const char* src)
{
	std::memcpy(&dest.id, src, sizeof(uint64_t));
	src += sizeof(uint64_t);
	std::memcpy(&dest.key, src, sizeof(uint64_t));
	src += sizeof(uint64_t);
	size_t record_size;
	std::memcpy(&record_size, src, sizeof(uint64_t));
	src += sizeof(uint64_t);
	dest.data.resize(record_size);
	std::memcpy(dest.data.data(), src, record_size);
	src += record_size;
	return src;
}

transaction decode_transaction(const Message& m)
{
	transaction tc(0);
	auto start = m.data.data();
	decode_transaction(tc, start);
	return tc;
}

p2p_node::p2p_node(uint16_t port, size_t id) : 
	context(),
	acceptor(context, tcp::endpoint(tcp::v4(), port)),
	garbage_collection_timer(context, garbage_collection_interval),
	heartbeat_send_timer(context, heartbeat_send_interval),
	heartbeat_process_timer(context, heartbeat_check_interval),
	my_info(localhost, port, id)
{
	connected_ids.push_back(id);
}

p2p_node::~p2p_node()
{
	for (auto& [con, id] : connection_to_id)
		cut_connection(id);
	context.stop();
	if (context_thrd.joinable())
		context_thrd.join();
}


void p2p_node::start()
{
	try
	{
		accept_connection();
		handle_garbage_collection();
		broadcast_heartbeat();
		handle_heartbeat();
		context_thrd = std::thread([this]() { context.run(); });
	}
	catch (std::exception e)
	{
		std::cerr << "[ERROR] Exception : " << e.what() << "\n";
	}

	std::cout << "[LOG] Started server [" << my_info.id << "]" << " in port " << my_info.port << ".\n";
}

void p2p_node::accept_connection()
{
	acceptor.async_accept([this](std::error_code ec, tcp::socket soc)
		{
			if (!ec)
			{
				std::unique_lock lock(connections_lock);
				auto new_connection = std::make_shared<TCPConnection>(context, std::move(soc), in_messages);
				new_connection->start_connection();
				temp_connections.push_back(std::move(new_connection));
			}
			else
			{
				std::cout << "[ERROR] " << ec.message() << "\n";
			}
			accept_connection();
		}
	);
}

void p2p_node::establish_connection(uint32_t host, uint16_t port)
{
	auto soc = tcp::socket(context);
	std::vector<tcp::endpoint> eps;
	eps.emplace_back(asio::ip::address_v4(host), port);
	std::error_code ec;
	asio::connect(soc, eps, ec);
	if (!ec)
	{
		std::unique_lock lock(connections_lock);
		auto new_connection = std::make_shared<TCPConnection>(context, std::move(soc), in_messages);
		new_connection->start_connection();
		message_format::intro payload;
		payload.id = my_info.id;
		payload.port = my_info.port;
		Message msg = build_message(message_context::intro, payload);
		new_connection->send_message(msg);

		temp_connections.push_back(std::move(new_connection));
		
	}
	else
	{
		std::cout << "[ERROR] Failed to connect to " << host << ":" << port << " because " << ec.message() << "\n";
	}
}

void p2p_node::connect_to_all()
{
	cleanup_connection_tables();
	std::shared_lock lock(table_lock);
	for (auto& [id, info] : machines)
	{
		if (established_connections.find(id) == established_connections.end())
		{
			establish_connection(info.host, info.port);
		}
	}
}

void p2p_node::add_new_machine(connection_ptr ptr, uint64_t id, uint16_t port)
{
	std::unique_lock lock(table_lock);
	connection_to_id[ptr] = id;
	established_connections[id] = ptr;
	machines[id] = machine_info(ptr->get_hostname(), port, id);
	connected_ids.insert(std::upper_bound(connected_ids.begin(), connected_ids.end(), id), id);
}

void p2p_node::add_new_machine(const machine_info &inf)
{
	std::unique_lock lock(table_lock);
	machines[inf.id] = inf;
}

void p2p_node::broadcast(const Message &m)
{
	std::shared_lock lock(table_lock);
	for (auto& [id, con] : established_connections)
	{
		if (con->active())
		{
			con->send_message(m);
		}
	}
}



void p2p_node::read_one_message()
{
	auto pulled = in_messages.pop();
	if (!pulled.has_value())
		return;
	auto &[con, msg] = *pulled;
	switch (msg.header.context)
	{
		case message_context::intro :
		case message_context::self_id :
		{
			message_format::intro content;
			read_message_data(content, msg);
			
			if (cut_connection(content.id))
				std::cout << "[WARNING] A machine with the same id as " << content.id << " exists. It has been forcibly disconnected.\n";

			//Message record_msg = build_message(message_context::record, machine_info(con->get_hostname(), content.port, content.id));
			//broadcast(record_msg);

			add_new_machine(con, content.id, content.port);
			if (msg.header.context == message_context::intro)
			{
				message_format::intro payload;
				payload.id = my_info.id;
				payload.port = my_info.port;
				Message id_msg = build_message(message_context::self_id, payload);
				con->send_message(id_msg);

				std::vector<machine_info> records;
				records.reserve(machines.size());
				for (auto& [id, info] : machines)
					records.push_back(info);
				Message record_msg = build_message_array<machine_info>(message_context::record, records);
				con->send_message(record_msg);
			}
			break;
		}
		case message_context::record :
		{
			std::vector<machine_info> content;
			read_message_array<machine_info>(content, msg);
			std::unique_lock guard(table_lock);
			for (auto& info : content)
			{
				if (info.id != my_info.id)
					machines[info.id] = info;
			}
			break;
		}
		case message_context::transaction_prepare :
		{
			transaction tc = decode_transaction(msg);
			{
				xstatus_accessor accessor;
				transaction_status_table.insert(accessor, tc.id);
				accessor->second = transaction_status::pending;
			}
			auto handle_new_transaction = [this, con, tc]()
			{
				data_accessor accessor;
				bool new_key = !data_storage.find(accessor, tc.key);
				if (new_key)
					data_storage.insert(accessor, tc.key);

				Message m = build_message(message_context::transaction_accept, tc.id);
				con->send_message(m);
				transaction_status stat = transaction_status::pending;

				std::unique_lock lock(cv_mutex);
				transaction_cv.wait(lock, [&]()
					{
						xstatus_caccessor stat_accessor;
						transaction_status_table.find(stat_accessor, tc.id);
						stat = (stat_accessor->second);
						stat_accessor.release();
						return stat != transaction_status::pending;
					}
				);

				if (stat == transaction_status::committed)
				{
					accessor->second = tc.data;
					if (new_key)
						keys.insert(tc.key);
					committed_transactions.push_back(tc);
					std::cout << "[LOG] Committed transaction " << tc.id << " successfully.\n";
				}
				else
				{
					if (new_key)
						data_storage.erase(accessor);
					std::cout << "[LOG] Aborted transaction " << tc.id << ".\n";
				}

			};
			std::thread t(handle_new_transaction);
			t.detach();
			break;
		}
		case message_context::transaction_accept :
		{
			size_t trans_id;
			read_message_data<size_t>(trans_id, msg);

			xans_accessor accessor;
			std::shared_lock tlock(table_lock);

			if (transaction_ans.find(accessor, trans_id))
			{
				accessor->second.insert(connection_to_id[con]);
			}
			else
			{
				std::cerr << "ERROR ACCEPT\n";
			}

			transaction_cv.notify_all();
			break;
		}
		case message_context::transaction_refuse:
		{
			size_t trans_id;
			read_message_data<size_t>(trans_id, msg);

			xstatus_accessor accessor;
			std::shared_lock tlock(table_lock);

			if (transaction_status_table.find(accessor, trans_id))
			{ 
				accessor->second = transaction_status::aborted;
			}
			else
			{
				std::cerr << "ERROR REFUSE\n";
			}
			

			transaction_cv.notify_all();
			break;
		}
		case message_context::transaction_commit :
		{
			size_t trans_id;
			read_message_data<size_t>(trans_id, msg);

			xstatus_accessor accessor;
			std::shared_lock tlock(table_lock);

			if (transaction_status_table.find(accessor, trans_id))
			{
				accessor->second = transaction_status::committed;
			}
			else
			{
				std::cerr << "ERROR COMMIT\n";
			}

			transaction_cv.notify_all();
			break;
		}
		case message_context::transaction_abort:
		{
			size_t trans_id;
			read_message_data<size_t>(trans_id, msg);

			xstatus_accessor accessor;
			std::shared_lock tlock(table_lock);

			transaction_status_table.find(accessor, trans_id);
			accessor->second = transaction_status::aborted;

			transaction_cv.notify_all();
			break;
		}
		case message_context::sync_ask:
		{
			message_format::sync_ask ask_msg;
			read_message_data(ask_msg, msg);
			sync_accessor accessor;
			std::cout << "[LOG] Received ask message " << ask_msg.id << ", " << ask_msg.seq << "\n";
			if (sync_progress.find(accessor, ask_msg.id))
			{
				std::cout << "[LOG] Moving sync process for machine " << ask_msg.id << " to " << ask_msg.seq << ".\n";
				accessor->second = ask_msg.seq;
				std::unique_lock cv_lock(sync_cv_mutex);
				sync_cv.notify_all();
			}
			else
			{
				std::cout << "[LOG] Starting sync process for machine " << ask_msg.id << " at " << ask_msg.seq << ".\n";
				sync_progress.insert(accessor, ask_msg.id);
				accessor->second = ask_msg.seq;
				accessor.release();
				auto handle_sync = [this, ask_msg, con]()
				{
					std::unique_lock u(global_transaction_lock, std::defer_lock);
					std::shared_lock s(global_transaction_lock);
					size_t seq_number = ask_msg.seq;
					size_t last_seq_number = -1;
					while (committed_transactions.size() > seq_number)
					{
						//check for an update
						bool final_phase = synchronization_batch_size > (committed_transactions.size() - seq_number);
						if (final_phase)
						{
							s.unlock();
							u.lock();
						}
						
						size_t total_size = 0;
						auto begin_it = committed_transactions.cbegin() + seq_number;
						auto end_it = final_phase ? committed_transactions.cend() : begin_it + synchronization_batch_size;
						size_t num_transactions_sent = final_phase ? (committed_transactions.size() - seq_number) : synchronization_batch_size;

						Message m;
						m.header.context = message_context::sync_ans;
						m.header.size += 16;
						m.data.resize(m.header.size);
						std::memcpy(m.data.data(), &num_transactions_sent, 8);
						std::memcpy(m.data.data() + 8, &seq_number, 8);
						for (auto it = begin_it; it != end_it; it++)
						{
							auto& tc = *it;
							size_t transaction_size = 3 * sizeof(uint64_t) + tc.data.size();
							m.data.resize(m.header.size + transaction_size);
							encode_transaction(m.data.data() + m.header.size, tc);
							m.header.size += transaction_size;
						}

						con->send_message(m);

						//Wait for answer.
						last_seq_number = seq_number;
						std::unique_lock cv_lock(sync_cv_mutex);
						sync_cv.wait(cv_lock, [&]()
							{
								sync_caccessor accessor;
								sync_progress.find(accessor, ask_msg.id);
								seq_number = accessor->second;
								return seq_number != last_seq_number;
							}
						);
					}
					Message done_message;
					done_message.header.context = message_context::sync_done;
					con->send_message(done_message);
					//Wait for termination recognition.
					std::unique_lock cv_lock(sync_cv_mutex);
					sync_cv.wait(cv_lock, [&]()
						{
							sync_caccessor accessor;
							sync_progress.find(accessor, ask_msg.id);
							return accessor->second == 0;
						}
					);
					sync_accessor delete_accessor;
					if (sync_progress.find(delete_accessor, ask_msg.id))
						sync_progress.erase(delete_accessor);
					std::cout << "Finished synchronization with machine " << ask_msg.id << " : " << seq_number << "/" << committed_transactions.size() << ".\n";
				};
				std::thread t(handle_sync);
				t.detach();
			}
			break;
		}
		case message_context::sync_ans:
		{
			size_t num_transactions_received;
			size_t seq_number;
			std::memcpy(&num_transactions_received, msg.data.data(), 8);
			std::memcpy(&seq_number, msg.data.data() + 8, 8);
			const char* cursor = msg.data.data() + 16;
			std::cout << "[LOG] Received " << num_transactions_received << "\n";
			for (size_t i = 0; i < num_transactions_received; i++)
			{
				transaction tc(0);
				cursor = decode_transaction(tc, cursor);
			}
			message_format::sync_ask payload;
			payload.id = my_info.id;
			payload.seq = seq_number + num_transactions_received;
			Message m = build_message(message_context::sync_ask, payload);
			con->send_message(m);
			break;
		}
		case message_context::sync_done:
		{
			Message ack = build_message(message_context::sync_done_ack, my_info.id);
			con->send_message(ack);
			std::cout << "Finished synchronization.\n";
			break;
		}
		case message_context::sync_done_ack:
		{
			size_t mid;
			read_message_data(mid, msg);
			sync_accessor accessor;
			if (sync_progress.find(accessor, mid))
			{
				accessor->second = 0;
				std::unique_lock cv_lock(sync_cv_mutex);
				sync_cv.notify_all();
			}
			break;
		}
		default:
			break;
	}
	if (connection_to_id.find(con) != connection_to_id.end())
		register_heartbeat(connection_to_id[con]);
	
}

std::optional<machine_info> p2p_node::get_machine_info(size_t id)
{
	std::shared_lock lock(table_lock);
	auto it = machines.find(id);
	if (it == machines.end())
		return std::nullopt;
	return it->second;
}

size_t p2p_node::query(size_t id)
{
	std::shared_lock lock(table_lock);
	auto it = std::upper_bound(connected_ids.begin(), connected_ids.end(), id);
	if (it == connected_ids.begin())
		return *(--connected_ids.end());
	return *(--it);
}

//Undefined behaviour if id is not in the table
int p2p_node::get_node_index(size_t id)
{
	std::shared_lock lock(table_lock);
	auto it = std::lower_bound(connected_ids.begin(), connected_ids.end(), id);
	int idx = std::distance(connected_ids.begin(), it);
	return idx;
}


//Garbage collection

bool p2p_node::cut_connection(size_t machine)
{
	std::unique_lock lock(table_lock);
	if (established_connections.find(machine) != established_connections.end())
	{
		auto& ptr = established_connections[machine];
		if (ptr->active())
			ptr->close();
		connection_to_id.erase(ptr);
		established_connections.erase(machine);
		return true;
	}
	return false;
}

void p2p_node::cleanup_temp_connections()
{
	std::unique_lock lock(connections_lock);
	std::shared_lock tbl_lock(table_lock);

	std::vector<connection_ptr> new_connections;
	size_t old_size = temp_connections.size();
	for (auto& con : temp_connections)
	{
		if (connection_to_id.find(con) == connection_to_id.end())
		{
			if (con->active())
				new_connections.push_back(con);
		}
	}
	size_t new_size = new_connections.size();
	temp_connections.clear();
	temp_connections = new_connections;
}

void p2p_node::cleanup_connection_tables()
{
	std::unique_lock lock(table_lock);
	for (auto it = established_connections.begin(); it != established_connections.end();)
	{
		auto& [id, con] = *it;
		if (con == nullptr || !con->active())
		{
			if (con != nullptr)
				connection_to_id.erase(con);
			connected_ids.erase(std::find(connected_ids.begin(), connected_ids.end(), id));
			it = established_connections.erase(it);
		}
		else
			it++;
	}
}

void p2p_node::handle_garbage_collection()
{
	garbage_collection_timer.expires_from_now(garbage_collection_interval);
	garbage_collection_timer.async_wait([this](std::error_code ec)
		{
			cleanup_temp_connections();
			cleanup_connection_tables();
			handle_garbage_collection();
		}
	);
}

//Heartbeat

void p2p_node::register_heartbeat(size_t id)
{
	std::unique_lock lock(heartbeat_lock);
	heartbeat_ids.insert(id);
}

void p2p_node::handle_heartbeat()
{
	heartbeat_process_timer.expires_from_now(heartbeat_check_interval);
	heartbeat_process_timer.async_wait([this](std::error_code ec)
		{
			std::unique_lock h_lock(heartbeat_lock);
			std::unique_lock tbl_lock(table_lock);
			for (auto it = established_connections.begin(); it != established_connections.end();)
			{
				auto& [id, con] = *it;
				if (heartbeat_ids.find(id) == heartbeat_ids.end())
				{
					if (con != nullptr)
					{
						con->close();
						connection_to_id.erase(con);
					}
					connected_ids.erase(std::find(connected_ids.begin(), connected_ids.end(), id));
					it = established_connections.erase(it);
				}
				else
				{
					it++;
				}
			}
			heartbeat_ids.clear();
			handle_heartbeat();
		}
	);

}

void p2p_node::broadcast_heartbeat()
{
	heartbeat_send_timer.expires_from_now(heartbeat_send_interval);
	garbage_collection_timer.async_wait([this](std::error_code ec)
		{
			Message m;
			m.header.context = message_context::heartbeat;
			m.header.size = 0;
			broadcast(m);
			broadcast_heartbeat();
		}
	);

}

//Commit

void p2p_node::start_2pc(const transaction &tc)
{
	size_t xid = tc.id;
	{
		xans_accessor ans_accessor;
		xstatus_accessor stat_accessor;

		transaction_ans.insert(ans_accessor, xid);
		ans_accessor->second = std::unordered_set<size_t>();
		transaction_status_table.insert(stat_accessor, xid);
		stat_accessor->second = transaction_status::pending;
	}

	auto work = [this, tc]()
	{
		size_t xid = tc.id;
		std::shared_lock global_xlock(global_transaction_lock);

		std::vector<size_t> replicas_id = std::vector<size_t>();

		int idx = get_node_index(my_info.id);
		{
			std::shared_lock contable_lock(table_lock);
			for (size_t i = 1; i < replication_count; i++)
			{
				replicas_id.push_back(connected_ids[(idx + i) % connected_ids.size()]);
			}
		}

		data_accessor accessor;
		bool new_key = !data_storage.find(accessor, tc.key);
		if (new_key)
			data_storage.insert(accessor, new_key);
		
		Message prepare_msg = encode_transaction(tc);
		for (auto replica : replicas_id)
		{
			established_connections[replica]->send_message(prepare_msg);
		}

		std::unique_lock lock(cv_mutex);
		transaction_cv.wait(lock, [&]()
			{
				bool finished = false;
				xans_caccessor ans_accessor;
				transaction_ans.find(ans_accessor, xid);
				finished |= ans_accessor->second.size() >= (replication_count - 1);
				ans_accessor.release();
				if (!finished)
				{
					xstatus_caccessor stat_accessor;
					transaction_status_table.find(stat_accessor, xid);
					bool cond2 = (stat_accessor->second) == transaction_status::aborted;
					finished |= cond2;
				}
				return finished;
			}
		);

		xstatus_accessor stat_accessor;
		transaction_status_table.find(stat_accessor, xid);
		if (stat_accessor->second == transaction_status::pending)
		{
			Message m = build_message(message_context::transaction_commit, tc.id);
			for (auto replica : replicas_id)
			{
				established_connections[replica]->send_message(m);
			}
			accessor->second = tc.data;
			if (new_key)
				keys.insert(tc.key);

			committed_transactions.push_back(tc);
			stat_accessor->second = transaction_status::committed;

			//std::cout << "[LOG] Committed transaction " << tc.id << " successfully.\n";
		}
		else
		{
			Message m = build_message(message_context::transaction_abort, tc.id);
			for (auto replica : replicas_id)
			{	
				established_connections[replica]->send_message(m);
			}
			if (new_key)
				data_storage.erase(accessor);
			std::cout << "[LOG] Aborted transaction " << tc.id << " successfully.\n";
		}
	};
	auto timeout_work = [this, tc]()
	{
		std::this_thread::sleep_for(commit_max_time);
		xstatus_accessor stat_accessor;
		transaction_status_table.find(stat_accessor, tc.id);
		if (stat_accessor->second == transaction_status::pending)
			stat_accessor->second = transaction_status::aborted;
		std::unique_lock lock(cv_mutex);
		transaction_cv.notify_all();
	};
	std::thread th(work);
	std::thread timeout(timeout_work);

	th.detach();
	timeout.detach();
}

//DEBUG
void p2p_node::log_machine_table()
{
	std::shared_lock lock(table_lock);
	for (auto& [id, info] : machines)
	{
		bool connected = (established_connections.find(id) != established_connections.end());
		std::cout << "[LOG] Machine " << id << " at " << info.host << ":" << info.port << ". Connection status " << connected << ".\n";
	}
	std::cout << "Current ring : ";
	for (auto id : connected_ids)
	{
		std::cout << id << ",";
	}
	std::cout << '\n';
}

void p2p_node::send_sync_to_random_node()
{
	message_format::sync_ask first_ask;
	first_ask.id = my_info.id;
	first_ask.seq = 0;
	Message m = build_message(message_context::sync_ask, first_ask);
	auto it = established_connections.begin();
	it->second->send_message(m);
}