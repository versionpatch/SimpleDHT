#include <asio.hpp>
#include <string>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <mutex>
#include <iostream>
#include <thread>
#include <random>

#include "NodeTable.h"
#include "Utils.h"
#include "TCPConnection.h"
#include "p2pnode.h"









int main()
{
	int serv_port;
	std::cin >> serv_port;
	std::cout << "\n";
	std::random_device rd;
	std::mt19937_64 mt(rd());
	uint64_t id = mt();
	id = serv_port * 10000;
	p2p_node server(serv_port, id);
	server.start();
	std::thread thd([&server]()
		{
			while (true)
			{
				server.read_one_message();
			}
		}
	);
	while (true)
	{
		int port;
		std::cin >> port;
		if (port == 0)
			server.log_machine_table();
		else if (port == 1)
			server.connect_to_all();
		else if (port == 2)
		{
			size_t k = mt();
			for (size_t i = 0; i < 300; i++)
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
				transaction t(k + i);
				t.key = id + i;
				t.data.push_back(0);
				server.start_2pc(t);
			}
		}
		else if (port == 3)
		{
			server.send_sync_to_random_node();
		}
		else if (port == 4)
		{
			server.log_data();
		}
		else
			server.join_ring(localhost, port);
	}
	
}