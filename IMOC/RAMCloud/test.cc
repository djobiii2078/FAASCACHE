#include <stdio.h>
#include "ClientException.h"
#include <iostream>
#include "RamCloud.h"

using namespace RAMCloud;
using namespace std;

int main(int argc, char *argv[])
try{

	if(argc != 2){
		fprintf(stderr,"Usage: %s coordinatorLocator", argv[0]);
	}

	RamCloud cluster(argv[1],"__unamed__");
	const char *pending_table = "pending_mig";
	//uint64_t table = cluster.createTable("Trap");
	
	//Create table pending_mig 
	uint64_t table = cluster.createTable(pending_table);
	cout <<"Table pending_mig successfully created ...."<< endl;
	cout <<"Testing pending_mig. Writing object xxx_testing_xxx ...."<< endl;
	
	const char *write_test_key_value = "xxx_testing_xxx";
	uint32_t test_char_len = downCast<uint32_t>(strlen(write_test_key_value));

	cluster.write(table, write_test_key_value, test_char_len, write_test_key_value, test_char_len);
	cout << write_test_key_value << " written successfully in table " << pending_table << endl;

	//cluster.write(table, "43",2,value, downCast<uint32_t>(strlen(value)+1));
	//cout <<"3. Written weird number successfully"<<endl;
	Buffer buffer;
	cluster.read(table,write_test_key_value,test_char_len,&buffer);
	//cluster.read(table,"42",2,&buffer);
	// cout <<"Value read " << buffer <<endl;

	//Test fetch service locator of the object-key-value
	cout << "Locator read : " << cluster.testingGetServiceLocator(table,write_test_key_value,test_char_len) << endl;
	cout << "Locator read (mine): " << cluster.scalaGetServiceLocator(table,write_test_key_value,test_char_len) << endl;


	cluster.remove(table,write_test_key_value,test_char_len);
	//cout <<"5. Dropping table, iiiiiiiiii. GoodBye"<<endl;
	
	cout <<"Delete completed successfully. RamCloud tests okay" << endl;

	return 0;

	
}catch(RAMCloud::ClientException& e){
	fprintf(stderr,"RAMCloud exception: %s\n", e.str().c_str());
	return 1;
}catch(RAMCloud::Exception& e){
	fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
	return 1;
}
