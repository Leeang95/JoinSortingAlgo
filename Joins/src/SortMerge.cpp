#include "join.h"
#include "scan.h"
#include <iostream>

using namespace std;

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
	Status stat;

	// Create temporary heapfile
	HeapFile *tmpHeap = new HeapFile(NULL, stat); if (stat != OK) {std::cerr << "Failed to create output heapfile." << std::endl; return FAIL;}

	/////// ***** Phase 1: Sorting ***** //////
	HeapFile *L = JoinMethod::SortHeapFile(left.file,left.recLen,left.offset);
	HeapFile *R = JoinMethod::SortHeapFile(right.file,right.recLen,right.offset);

	// Open scan on left relation
	Scan *leftScan = L->OpenScan(stat); if (stat != OK) {std::cerr << "Failed to open scan on left relation." << std::endl; return FAIL;}

	// Open scan on right relation
	Scan *rightScan = R->OpenScan(stat); if (stat != OK) {std::cerr << "Failed to open scan on right relation." << std::endl;return FAIL;}

	////// ***** Phase 2: Merging ***** //////
	RecordID leftrid, rightrid, insertedrid, fixLeft, fixRight, eopLeft, eopRight;
	int *leftJoinAttr = new int();
	int *rightJoinAttr = new int();
    int *leftJoinAttrPrime = new int();
	int *rightJoinAttrPrime = new int();
	char *leftarr = new char[left.recLen];
	char *rightarr = new char[right.recLen];
	char *joinedarr = new char[out.recLen];

	// Get the first tuples from the left and right relations.
	stat = leftScan->GetNext(leftrid,leftarr,left.recLen); if (stat != OK) {std::cout << "TRACER    1" << std::endl; return FAIL;}
	stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {std::cout << "TRACER    2" << std::endl; return FAIL;}
	*leftJoinAttr = *leftJoinAttrPrime = *(int*)(leftarr + left.offset);
	*rightJoinAttr = *rightJoinAttrPrime = *(int*)(rightarr + right.offset);

	while (!leftScan->noMore && !rightScan->noMore) {
		if (*leftJoinAttr < *rightJoinAttr) {
			stat = leftScan->GetNext(leftrid,leftarr,left.recLen); if (stat != OK) {std::cout << "TRACER    3" << std::endl; return FAIL;}
			*leftJoinAttr = *leftJoinAttrPrime = *(int*)(leftarr + left.offset);
		}
		else if (*leftJoinAttr > *rightJoinAttr) {
			stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {std::cout << "TRACER    4" << std::endl; return FAIL;}
			*rightJoinAttr = *rightJoinAttrPrime = *(int*)(rightarr + right.offset);
		}
		else {
			// Mark beginning of each LEFT and RIGHT partition.
			fixLeft = eopLeft = leftScan->currRid;
			fixRight = eopRight = rightScan->currRid;

			// Continue scanning current RIGHT relation to find the end of the RIGHT partition (eopRight is index of first element after partition).
			while (!rightScan->noMore && rightJoinAttr == rightJoinAttrPrime) {
				stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {std::cout << "TRACER    5" << std::endl; return FAIL;}
				rightJoinAttrPrime = (int*)(rightarr + right.offset);
				eopRight = rightScan->currRid;
			}
			rightScan->MoveTo(fixRight);
		    stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {std::cout << "TRACER    6" << std::endl; return FAIL;}

			// Continue scanning current LEFT relation to find the end of the LEFT partition (eopLeft is index of first element after partition).
			while (!leftScan->noMore && leftJoinAttr == leftJoinAttrPrime) {
				stat = leftScan->GetNext(leftrid,leftarr,left.recLen); if (stat != OK) {std::cout << "TRACER    7" << std::endl; return FAIL;}
				leftJoinAttrPrime = (int*)(leftarr + left.offset);
				eopLeft = leftScan->currRid;
			}
			leftScan->MoveTo(fixLeft);
	        stat = leftScan->GetNext(leftrid,leftarr,left.recLen); if (stat != OK) {std::cout << "TRACER    8" << std::endl; return FAIL;}

			// For each element of LEFT partition, join with each element of RIGHT partition.
			while (!leftScan->noMore && leftScan->currRid != eopLeft) {
				while (!rightScan->noMore && rightScan->currRid != eopRight) {
					JoinMethod::MakeNewRecord(joinedarr,leftarr,rightarr,left,right);
					stat = tmpHeap->InsertRecord(joinedarr,out.recLen,insertedrid); if (stat != OK) {std::cout << "TRACER    9" << std::endl; return FAIL;}
					stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {std::cout << "TRACER    10" << std::endl; return FAIL;}
					if (rightScan->noMore) {
						JoinMethod::MakeNewRecord(joinedarr,leftarr,rightarr,left,right);
						stat = tmpHeap->InsertRecord(joinedarr,out.recLen,insertedrid); if (stat != OK) {std::cout << "TRACER    11" << std::endl; return FAIL;}
					}
				}
				rightScan->MoveTo(fixRight);
				stat = leftScan->GetNext(leftrid,leftarr,left.recLen); if (stat != OK) {std::cout << "TRACER    12" << std::endl; return FAIL;}
				if (leftScan->noMore) {
					JoinMethod::MakeNewRecord(joinedarr,leftarr,rightarr,left,right);
						stat = tmpHeap->InsertRecord(joinedarr,out.recLen,insertedrid); if (stat != OK) {std::cout << "TRACER    13" << std::endl; return FAIL;}
				}
			}
			rightScan->MoveTo(eopRight);
			stat = rightScan->GetNext(rightrid,rightarr,right.recLen); if (stat != OK) {std::cout << "TRACER    14" << std::endl; return FAIL;}
		}
	}
	
	out.file = tmpHeap;
	delete[] leftarr;
	delete[] rightarr;
	delete[] joinedarr;
	delete leftJoinAttr;
	delete rightJoinAttr;
    delete leftJoinAttrPrime;
	delete rightJoinAttrPrime;
	return OK;
}