#include "join.h"
#include "scan.h"
#include <iostream>

//---------------------------------------------------------------
// SortMerge::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs an sort merge join on the specified relations. 
// Please see the pseudocode on page 460 of your text for more info
// on this algorithm. You may use JoinMethod::SortHeapFile to sort
// the relations. 
//---------------------------------------------------------------
Status SortMerge::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
	JoinMethod::Execute(left, right, out);

	// Create temporary heapfile
	Status stat;
	HeapFile *tmpHeap = new HeapFile(NULL, stat);
	if (stat != OK) {
		std::cerr << "Failed to create output heapfile." << std::endl;
		return FAIL;
	}

	// Open scan on left relation
	Scan *leftScan = left.file->OpenScan(stat);
	if (stat != OK) {
		std::cerr << "Failed to open scan on left relation." << std::endl;
		return FAIL;
	}

	// Open scan on right relation
	Scan *rightScan = right.file->OpenScan(stat);
	if (stat != OK) {
		std::cerr << "Failed to open scan on right relation." << std::endl;
		return FAIL;
	}

	/////// ***** Phase 1: Sorting ***** //////
	JoinMethod::SortHeapFile(left.file,left.recLen,left.offset);
	JoinMethod::SortHeapFile(right.file,right.recLen,right.offset);

	////// ***** Phase 2: Merging ***** //////
	RecordID leftrid, rightrid, insertedrid;
	int *leftJoinAttr, *rightJoinAttr;
	char *leftarr = new char[left.recLen];
	char *rightarr = new char[right.recLen];
	char *joinedarr = new char[left.recLen + right.recLen];

	// Get the first tuples from the left and right relations.
	stat = leftScan->GetNext(leftrid,leftarr,left.recLen); if (stat != OK) {return FAIL; std::cout << "TRACER 1" << std::endl;}
	stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {return FAIL; std::cout << "TRACER 2" << std::endl;}
	leftJoinAttr = (int*)(leftarr + left.offset);
	rightJoinAttr = (int*)(rightarr + right.offset);

	while (!leftScan->noMore && !rightScan->noMore) {
		if (leftJoinAttr > rightJoinAttr) {
			stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {return FAIL; std::cout << "TRACER 3" << std::endl;}
			rightJoinAttr = (int*)(rightarr + right.offset);
			std::cout << "TRACERx" << std::endl;
		}
		else if (leftJoinAttr < rightJoinAttr) {
			stat = leftScan->GetNext(leftrid,leftarr,left.recLen); if (stat != OK) {return FAIL; std::cout << "TRACER 4" << std::endl;}
			leftJoinAttr = (int*)(leftarr + left.offset);
			std::cout << "TRACERy" << std::endl;
		}
		else {
			// Create the record and insert into tmpHeap...
			MakeNewRecord(joinedarr,leftarr,rightarr,left,right);
			stat = tmpHeap->InsertRecord(joinedarr,out.recLen,insertedrid); if (stat != OK) {return FAIL; std::cout << "TRACER 5" << std::endl;}
		
			// Continue searching for matching tuples on the right and insert them into tmpHeap.
			stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {return FAIL; std::cout << "TRACER 6" << std::endl;}
			rightJoinAttr = (int*)(rightarr + right.offset);
			while (!rightScan->noMore && leftJoinAttr == rightJoinAttr) {
				MakeNewRecord(joinedarr,leftarr,rightarr,left,right);
			    stat = tmpHeap->InsertRecord(joinedarr,out.recLen,insertedrid); if (stat != OK) {return FAIL; std::cout << "TRACER 7" << std::endl;}
                stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {return FAIL; std::cout << "TRACER 8" << std::endl;}
			    rightJoinAttr = (int*)(rightarr + right.offset);
				std::cout << "TRACERa" << std::endl;
			}

			// Continue searching for matching tuples on the left and insert them into tmpHeap.
			stat = leftScan->GetNext(leftrid,leftarr,left.recLen); if (stat != OK) {return FAIL; std::cout << "TRACER 9" << std::endl;}
		    leftJoinAttr = (int*)(leftarr + left.offset);
			while (!leftScan->noMore && leftJoinAttr == rightJoinAttr) {
				MakeNewRecord(joinedarr,leftarr,rightarr,left,right);
			    stat = tmpHeap->InsertRecord(joinedarr,out.recLen,insertedrid); if (stat != OK) {return FAIL; std::cout << "TRACER 10" << std::endl;}
                stat = leftScan->GetNext(leftrid,leftarr,left.recLen); if (stat != OK) {return FAIL; std::cout << "TRACER 11" << std::endl;}
			    leftJoinAttr = (int*)(leftarr + left.offset);
				std::cout << "TRACERb" << std::endl;
			}
			std::cout << "TRACERz" << std::endl;
		}
		stat = leftScan->GetNext(leftrid,leftarr,left.recLen); if (stat != OK) {return FAIL; std::cout << "TRACER 12" << std::endl;}
		stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {return FAIL; std::cout << "TRACER 13" << std::endl;}
	}

	out.file = tmpHeap;
	delete[] leftarr;
	delete[] rightarr;
	delete[] joinedarr;
	return OK;
}