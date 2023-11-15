#include "p2pnode.h"
#include <chrono>




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

			Message record_msg = build_message(message_context::record, machine_info(con->get_hostname(), content.port, content.id));
			broadcast(record_msg);

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
			size_t trans_id;
			read_message_data<size_t>(trans_id, msg);
			Message m = build_message(message_context::transaction_accept, trans_id);
			con->send_message(m);
			break;
		}
		case message_context::transaction_accept :
		{
			size_t trans_id;
			read_message_data<size_t>(trans_id, msg);

			xans_accessor accessor;
			std::shared_lock tlock(table_lock);

			transaction_ans.find(accessor, trans_id);
			accessor->second.insert(connection_to_id[con]);

			transaction_cv.notify_all();
			break;
		}
		case message_context::transaction_refuse:
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
		case message_context::transaction_commit :
		{
			size_t trans_id;
			read_message_data<size_t>(trans_id, msg);
			std::cout << "[LOG] Committed " << trans_id << "\n";
			break;
		}
		case message_context::transaction_abort:
		{
			size_t trans_id;
			read_message_data<size_t>(trans_id, msg);
			std::cout << "[LOG] Aborted " << trans_id << "\n";
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
	auto work = [this, tc]()
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

		std::vector<size_t> replicas_id = std::vector<size_t>();

		int idx = get_node_index(my_info.id);
		{
			std::shared_lock contable_lock(table_lock);
			for (size_t i = 1; i < replication_count; i++)
			{
				replicas_id.push_back(connected_ids[(idx + i) % connected_ids.size()]);
			}
		}

		for (auto replica : replicas_id)
		{
			Message m = build_message(message_context::transaction_prepare, xid);
			established_connections[replica]->send_message(m);
		}

		while (true)
		{
			std::unique_lock lock(cv_mutex);
			bool finished = false;
			{
				{
					xans_caccessor ans_accessor;
					transaction_ans.find(ans_accessor, xid);
					finished |= ans_accessor->second.size() >= (replication_count - 1);
				}
				if (!finished)
				{
					xstatus_caccessor stat_accessor;
					transaction_status_table.find(stat_accessor, xid);
					bool cond2 = (stat_accessor->second) == transaction_status::aborted;
				}
			}
			if (finished)
			{
				break;
			}
			transaction_cv.wait(lock);
		}

		xstatus_accessor stat_accessor;
		transaction_status_table.find(stat_accessor, xid);
		if (stat_accessor->second == transaction_status::pending)
		{
			for (auto replica : replicas_id)
			{
				Message m = build_message(message_context::transaction_commit, tc.id);
				established_connections[replica]->send_message(m);
			}
			stat_accessor->second = transaction_status::committed;
			std::cout << "[LOG] Committed transaction " << tc.id << " successfully.\n";
		}
		else
		{
			for (auto replica : replicas_id)
			{
				Message m = build_message(message_context::transaction_abort, tc.id);
				established_connections[replica]->send_message(m);
			}
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