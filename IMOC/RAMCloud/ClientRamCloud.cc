#include <stdio.h>
#include "ClientException.h"
#include <iostream>
#include "RamCloud.h"

using namespace RAMCloud;
//using namespace std;

int main(int argc, char *argv[])
try{

	if(argc != 2){
		fprintf(stderr,"Usage: %s coordinatorLocator", argv[0]);
	}

	RamCloud cluster(argv[1],"__unamed__");
	uint64_t table = cluster.createTable("Trap");
	std::cout <<"1. Table created"<< std::endl;
	cluster.write(table, "42",2, "Hello, World!", 14);
	const char *value = "12345689123456789";

	cluster.write(table, "43",2,value, downCast<uint32_t>(strlen(value)+1));
	std::cout <<"3. Written weird number successfully"<<std::endl;
	Buffer buffer;
	cluster.read(table,"43",2,&buffer);
	cluster.read(table,"42",2,&buffer);
	std::cout <<"4. Buffer initialized succesfully, try to print it :) "<<std::endl;
	cluster.dropTable("Trap");
	std::cout <<"5. Dropping table, iiiiiiiiii. GoodBye"<<std::endl;
	return 0;

	
}catch(RAMCloud::ClientException& e){
	fprintf(stderr,"RAMCloud exception: %s\n", e.str().c_str());
	return 1;
}catch(RAMCloud::Exception& e){
	fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
	return 1;
}
