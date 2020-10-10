#include <stdio.h>
#include "ClientException.h"
#include "CycleCounter.h"
#include <iostream>
#include "RamCloud.h"
#define NUM_SIZES 5
#define KEY_LENGTH 20
#define SCALING_UP 1 
//#define SCALING_DONW 2

using namespace RAMCloud;

int main(int argc, char *argv[])
try{

	if(argc != 2){
		fprintf(stderr,"Usage: %s coordinatorLocator", argv[0]);
	}

	RamCloud cluster(argv[1],"__unamed__");
	const char *pending_table = "scalingBench";
	//uint64_t table = cluster.createTable("Trap");
	
	//Create table pending_mig 
	uint64_t table = cluster.createTable(pending_table);
	std::cout <<"Table pending_mig successfully created ...."<< std::endl;
	std::cout <<"Starting benchmark ....."<< std::endl;

	int sizes[NUM_SIZES] = {100, 1024, 10*1024, 100*1024, 1024*1024};
	const char* ids[NUM_SIZES] = {"100", "1K", "10K", "100K", "1M"};

	int scalingSizes[NUM_SIZES] = {8*1024*1024,64*1024*1024,256*1024*1024,512*1024*1024,1024*1024*1024};
	const char* idSizes[NUM_SIZES] = {"8","64","256","512","1024"};
	uint64_t objectSize = 0; 
	uint64_t nbObjects = 0; 
	uint64_t start = 0;

	
	#ifdef SCALING_UP 
		std::cout <<"Populating table with 500MB" << idSizes << "of " << ids[NUM_SIZES-1] << " object sizes"<<std::endl; 
		nbObjects = 200000000/(sizes[NUM_SIZES-1]+KEY_LENGTH+20);
		cluster.testingFill(table,"",KEY_LENGTH,nbObjects,sizes[NUM_SIZES-1]);
		std::cout <<"Populating finished .... Starting scaling tests"<<std::endl;

		for (int i = NUM_SIZES-1; i >= 0; i--) {
			
			
			start = Cycles::rdtsc();
			cluster.benchmarkScaling(true,idSizes[i],std::strlen(idSizes[i]));
			std::cout <<"Scaled up in "<<Cycles::toMicroseconds(Cycles::rdtsc()-start)<< std::endl;

		}
	#endif 

	#ifdef SCALING_DOWN
		std::cout <<"Populating table with 500MB" << idSizes << "of " << ids[NUM_SIZES-1] << " object sizes"<<std::endl; 
		nbObjects = 200000000/(sizes[NUM_SIZES-1]+KEY_LENGTH+20);
		cluster.testingFill(table,"",KEY_LENGTH,nbObjects,sizes[NUM_SIZES-1]);
		std::cout <<"Populating finished .... Starting scaling tests"<<std::endl;

		for (int i = NUM_SIZES-1; i >= 0; i--) {
			

			start = Cycles::rdtsc();
			cluster.benchmarkScaling(false,idSizes[i],std::strlen(idSizes[i]));
			std::cout <<"Scaled up in "<<Cycles::toMicroseconds(Cycles::rdtsc()-start)<< std::endl;

		}
	#endif 

	cluster.dropTable(pending_table);

	return 0;

	
}catch(RAMCloud::ClientException& e){
	fprintf(stderr,"RAMCloud exception: %s\n", e.str().c_str());
	return 1;
}catch(RAMCloud::Exception& e){
	fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
	return 1;
}
