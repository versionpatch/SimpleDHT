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
			size_t i = 0;
			while (true)
			{
				std::this_thread::sleep_for(std::chrono::microseconds(300));
				transaction t(i);
				t.key = i % 3000;
				t.data.push_back(0);
				server.start_2pc(t);
				i++;
			}
		}
		else if (port == 3)
		{
			server.send_sync_to_random_node();
		}
		else
			server.establish_connection(localhost, port);
	}
	
}